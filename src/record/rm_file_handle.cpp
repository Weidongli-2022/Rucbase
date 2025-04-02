/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"
#include <limits>

// 定义常量
const int RM_NO_FREE_PAGE = -1;

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // 获取包含记录的页面
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查记录是否存在（通过位图检查）
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Record does not exist at RID(" + 
                               std::to_string(rid.page_no) + "," + 
                               std::to_string(rid.slot_no) + ")");
    }
    
    // 计算记录在页面中的位置
    char *record_data = page_handle.get_slot(rid.slot_no);
    
    // 创建记录对象，复制数据
    std::unique_ptr<RmRecord> record(new RmRecord(file_hdr_.record_size));
    memcpy(record->data, record_data, file_hdr_.record_size);
    
    // 解除页面固定
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // 获取或创建一个有空闲空间的页面
    RmPageHandle page_handle = create_page_handle();
    Page* page = page_handle.page;
    
    // 找到页面中第一个空闲的槽位
    int slot_no = Bitmap::first_bit(0, page_handle.bitmap, file_hdr_.num_records_per_page);
    
    // 如果没有找到空闲槽位
    if (slot_no == std::numeric_limits<int>::max()) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Failed to find free slot in page");
    }
    
    // 设置位图中对应的位，表示槽位已被使用
    Bitmap::set(page_handle.bitmap, slot_no);
    
    // 获取槽位位置并复制记录数据
    char *slot = page_handle.get_slot(slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    
    // 更新页面头部记录数量
    page_handle.page_hdr->num_records++;
    
    // 创建记录ID
    Rid new_rid = {page->get_page_id().page_no, slot_no};
    
    // 检查页面是否已满
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // 页面已满，需要更新文件头部的第一个空闲页
        if (file_hdr_.first_free_page_no == page->get_page_id().page_no) {
            file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        }
    }
    
    // 将页面标记为脏页，确保更改被写回磁盘
    buffer_pool_manager_->unpin_page(page->get_page_id(), true);
    
    return new_rid;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 获取包含记录的页面
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查记录是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Record does not exist at RID(" + 
                               std::to_string(rid.page_no) + "," + 
                               std::to_string(rid.slot_no) + ")");
    }
    
    // 清除位图中对应的位，表示槽位可复用
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    
    // 更新页面头部记录数量
    page_handle.page_hdr->num_records--;
    
    // 检查页面状态变化
    bool was_full = (page_handle.page_hdr->num_records + 1 == file_hdr_.num_records_per_page);
    
    // 如果页面从已满变为未满，更新空闲页链表
    if (was_full) {
        release_page_handle(page_handle);
    } else {
        // 将页面标记为脏页
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    }
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 获取包含记录的页面
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查记录是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Record does not exist at RID(" + 
                               std::to_string(rid.page_no) + "," + 
                               std::to_string(rid.slot_no) + ")");
    }
    
    // 获取槽位位置并更新记录数据
    char *slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    
    // 将页面标记为脏页，确保更改被写回磁盘
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // 使用缓冲池获取指定页面
    PageId page_id = {fd_, page_no};
    Page *page = buffer_pool_manager_->fetch_page(page_id);
    if (page == nullptr) {
        throw std::runtime_error("Failed to fetch page " + std::to_string(page_no));
    }
    
    // 初始化RmPageHandle
    RmPageHandle page_handle = {&file_hdr_, page};
    
    // 获取页头指针位置
    char *data = page->get_data();
    page_handle.page_hdr = reinterpret_cast<RmPageHdr*>(data);
    
    // 设置位图的起始位置
    page_handle.bitmap = data + sizeof(RmPageHdr);
    
    return page_handle;
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // 使用缓冲池创建新页面
    PageId page_id = {fd_, INVALID_PAGE_ID};
    Page *page = buffer_pool_manager_->new_page(&page_id);
    if (page == nullptr) {
        throw std::runtime_error("Failed to create new page");
    }
    
    // 初始化RmPageHandle
    RmPageHandle page_handle = {&file_hdr_, page};
    
    // 获取页头指针位置（页数据的开始位置）
    char *data = page->get_data();
    page_handle.page_hdr = reinterpret_cast<RmPageHdr*>(data);
    
    // 初始化页面头部信息
    page_handle.page_hdr->next_free_page_no = RM_NO_FREE_PAGE;
    page_handle.page_hdr->num_records = 0;
    
    // 设置位图的起始位置（紧接在页头之后）
    page_handle.bitmap = data + sizeof(RmPageHdr);
    
    // 初始化位图，将所有位设置为0（表示没有记录）
    Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);
    
    // 更新文件头部信息中的页面数量
    file_hdr_.num_pages++;
    
    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // 检查文件中是否有空闲页面
    if (file_hdr_.first_free_page_no != RM_NO_FREE_PAGE) {
        // 有空闲页面，直接获取第一个空闲页面
        return fetch_page_handle(file_hdr_.first_free_page_no);
    } else {
        // 没有空闲页面，创建新页面
        return create_new_page_handle();
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // 更新页面头部中的下一个空闲页信息
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    
    // 更新文件头部中的第一个空闲页为当前页
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
    
    // 将页面标记为脏页，确保更改被写回磁盘
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

