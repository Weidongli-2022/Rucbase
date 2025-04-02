/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief RmScan构造函数
 * @param file_handle 文件句柄
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // 初始化为第一个页的第一个槽位之前
    // 从第一个页面开始，槽位设为-1，调用next()会从0开始
    rid_.page_no = 0;
    rid_.slot_no = -1;
    
    // 调用next()找到第一个有效记录
    next();
}

/**
 * @brief 移动到下一个有效记录
 */
void RmScan::next() {
    // 检查文件是否为空（没有页面）
    if (file_handle_->file_hdr_.num_pages <= 1) {  // 第0页是文件头
        rid_.page_no = -1;  // 设置为扫描结束标志
        return;
    }
    
    // 从第1页开始扫描（第0页是文件头）
    if (rid_.page_no == 0 && rid_.slot_no == -1) {
        rid_.page_no = 1;  // 从第1页开始
        rid_.slot_no = -1; // 下面会加1变成0
    }
    
    // 增加槽位编号
    rid_.slot_no++;
    
    // 遍历所有页面查找有效记录
    while (rid_.page_no < file_handle_->file_hdr_.num_pages) {
        // 获取当前页面句柄
        RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);
        
        // 获取每页的记录数
        int records_per_page = file_handle_->file_hdr_.num_records_per_page;
        
        // 在当前页中继续查找有效记录
        while (rid_.slot_no < records_per_page) {
            // 检查位图，判断槽位是否被使用
            if (Bitmap::is_set(page_handle.bitmap, rid_.slot_no)) {
                // 找到有效记录，返回
                return;
            }
            // 继续下一个槽位
            rid_.slot_no++;
        }
        
        // 当前页已遍历完，移动到下一页
        rid_.page_no++;
        rid_.slot_no = 0;
    }
    
    // 所有页面遍历完毕，标记扫描结束
    rid_.page_no = -1;
    rid_.slot_no = -1;
}

/**
 * @brief 判断是否到达文件末尾
 * @return 是否结束
 */
bool RmScan::is_end() const {
    // 使用page_no为-1作为结束标志
    return rid_.page_no == -1;
}

/**
 * @brief 获取当前记录的RID
 * @return 当前记录的RID
 */
Rid RmScan::rid() const {
    // 如果扫描已结束，返回无效RID
    if (is_end()) {
        throw std::runtime_error("Scan has ended");
    }
    return rid_;
}