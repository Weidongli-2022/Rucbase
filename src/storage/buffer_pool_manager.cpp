/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

/**
 * @description: 寻找一个可替换的帧
 * @param {frame_id_t*} frame_id 如果找到，则存储可替换帧的帧号
 * @return {bool} 是否找到可替换帧
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // 首先检查是否有空闲帧可用
    if (!free_list_.empty()) {
        // 有空闲帧，直接使用
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    
    // 没有空闲帧，尝试使用replacer找到一个可淘汰的帧
    if (replacer_->victim(frame_id)) {
        // 找到了可淘汰的帧
        Page* victim_page = &pages_[*frame_id];
        
        // 如果是脏页，需要先写回磁盘
        if (victim_page->is_dirty_) {
            disk_manager_->write_page(victim_page->id_.fd, 
                                      victim_page->id_.page_no, 
                                      victim_page->data_, 
                                      PAGE_SIZE);
            victim_page->is_dirty_ = false;
        }
        
        // 从页表中删除对应的映射关系
        page_table_.erase(victim_page->id_);
        
        return true;
    }
    
    // 没有可用帧也没有可淘汰的帧
    return false;
}

/**
 * @description: 更新页面的元数据
 * @param {Page*} page 要更新的页面
 * @param {PageId} new_page_id 新的页面id
 * @param {frame_id_t} new_frame_id 新的帧id
 */
void BufferPoolManager::update_page(Page* page, PageId new_page_id, frame_id_t new_frame_id) {
    // 更新页面的id
    page->id_ = new_page_id;
    // 注意：Page类没有frame_id_成员变量，移除此行
    // page->frame_id_ = new_frame_id; 
    
    // 在BufferPoolManager中，frame_id通过page_table_映射表维护
    // 不需要在Page对象中直接存储
}

/**
 * @description: 创建一个新的页面
 * @param {PageId*} page_id 输出参数，新页面的id
 * @return {Page*} 新页面的指针，如果创建失败则返回nullptr
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    std::lock_guard<std::mutex> lock(latch_);  // 加锁保证线程安全
    
    frame_id_t frame_id;
    // 寻找可用帧
    if (!find_victim_page(&frame_id)) {
        // 没有可用帧
        return nullptr;
    }
    
    // 使用DiskManager分配页面号
    page_id->page_no = disk_manager_->allocate_page(page_id->fd);
    
    // 获取对应帧的页面
    Page* page = &pages_[frame_id];
    
    // 更新页面信息
    update_page(page, *page_id, frame_id);
    
    // 初始化页面状态
    page->pin_count_ = 1;  // 新页面被固定一次
    page->is_dirty_ = false;  // 新页面初始不是脏页
    
    // 更新页表
    page_table_[*page_id] = frame_id;
    
    // 通知替换器该帧已被固定
    replacer_->pin(frame_id);
    
    return page;
}

/**
 * @description: 从缓冲池或磁盘获取指定页面
 * @param {PageId} page_id 要获取的页面id
 * @return {Page*} 页面指针，如果获取失败则返回nullptr
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    std::lock_guard<std::mutex> lock(latch_);  // 加锁保证线程安全
    
    // 检查页面是否在缓冲池中
    auto iter = page_table_.find(page_id);
    if (iter != page_table_.end()) {
        // 页面已在缓冲池中
        frame_id_t frame_id = iter->second;
        Page* page = &pages_[frame_id];
        
        // 增加页面引用计数
        page->pin_count_++;
        
        // 通知替换器该帧已被固定
        replacer_->pin(frame_id);
        
        return page;
    }
    
    // 页面不在缓冲池中，需要从磁盘读取
    
    // 寻找可用帧
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
        // 没有可用帧
        return nullptr;
    }
    
    // 获取对应帧的页面
    Page* page = &pages_[frame_id];
    
    // 更新页面信息
    update_page(page, page_id, frame_id);
    
    // 从磁盘读取页面数据
    disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    
    // 初始化页面状态
    page->pin_count_ = 1;  // 页面被固定一次
    page->is_dirty_ = false;  // 从磁盘读取的页面初始不是脏页
    
    // 更新页表
    page_table_[page_id] = frame_id;
    
    // 通知替换器该帧已被固定
    replacer_->pin(frame_id);
    
    return page;
}

/**
 * @description: 取消固定一个页面
 * @param {PageId} page_id 要取消固定的页面id
 * @param {bool} is_dirty 页面是否被修改
 * @return {bool} 是否成功取消固定
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);  // 加锁保证线程安全
    
    // 检查页面是否在缓冲池中
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end()) {
        // 页面不在缓冲池中
        return false;
    }
    
    frame_id_t frame_id = iter->second;
    Page* page = &pages_[frame_id];
    
    // 检查引用计数
    if (page->pin_count_ <= 0) {
        // 引用计数已为0，无法再次取消固定
        return false;
    }
    
    // 如果页面被修改，标记为脏页
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    
    // 减少引用计数
    page->pin_count_--;
    
    // 如果引用计数减为0，通知替换器该帧可被替换
    if (page->pin_count_ == 0) {
        replacer_->unpin(frame_id);
    }
    
    return true;
}

/**
 * @description: 删除指定页面
 * @param {PageId} page_id 要删除的页面id
 * @return {bool} 是否成功删除
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    std::lock_guard<std::mutex> lock(latch_);  // 加锁保证线程安全
    
    // 检查页面是否在缓冲池中
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end()) {
        // 页面不在缓冲池中，视为删除成功
        return true;
    }
    
    frame_id_t frame_id = iter->second;
    Page* page = &pages_[frame_id];
    
    // 检查引用计数
    if (page->pin_count_ > 0) {
        // 页面仍在使用中，无法删除
        return false;
    }
    
    // 从页表中删除页面
    page_table_.erase(page_id);
    
    // 重置页面状态
    page->is_dirty_ = false;
    page->id_.fd = INVALID_PAGE_ID;  // 使用无效值标记
    page->id_.page_no = INVALID_PAGE_ID;
    
    // 将帧添加到空闲列表
    free_list_.push_back(frame_id);
    
    // 从替换器中移除该帧（因为它现在是空闲的）
    replacer_->pin(frame_id);  // 使用pin操作从替换器中移除
    
    return true;
}

/**
 * @description: 将指定页面刷新到磁盘
 * @param {PageId} page_id 要刷新的页面id
 * @return {bool} 是否成功刷新
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    std::lock_guard<std::mutex> lock(latch_);  // 加锁保证线程安全
    
    // 检查页面是否在缓冲池中
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end()) {
        // 页面不在缓冲池中
        return false;
    }
    
    frame_id_t frame_id = iter->second;
    Page* page = &pages_[frame_id];
    
    // 无论页面是否为脏页，都将其写回磁盘
    disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    
    // 重置脏页标记
    page->is_dirty_ = false;
    
    return true;
}

/**
 * @description: 将指定文件的所有页面刷新到磁盘
 * @param {int} fd 文件描述符
 */
void BufferPoolManager::flush_all_pages(int fd) {
    std::lock_guard<std::mutex> lock(latch_);  // 加锁保证线程安全
    
    // 遍历所有页面
    for (size_t i = 0; i < pool_size_; i++) {
        Page* page = &pages_[i];
        
        // 检查页面是否属于指定文件
        if (page->id_.fd == fd) {
            // 无论页面是否为脏页，都将其写回磁盘
            disk_manager_->write_page(fd, page->id_.page_no, page->data_, PAGE_SIZE);
            
            // 重置脏页标记
            page->is_dirty_ = false;
        }
    }
}

