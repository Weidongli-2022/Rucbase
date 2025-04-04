/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"
#include "sm_defs.h"
#include "index/ix_manager.h"
#include "record/rm_manager.h"
#include "transaction/transaction.h"
#include "errors.h"  // 包含错误定义

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string &tab_name, 
                           const std::vector<std::string> &col_names,
                           Context *context) {
    // 1. 创建IxManager实例
    IxManager ix_manager(disk_manager_, buffer_pool_manager_);
    
    // 2. 检查表是否存在并获取表元数据
    TabMeta &tab = db_.get_table(tab_name);
    if (tab.name.empty()) {
        throw TableNotFoundError(tab_name);
    }
    
    // 3. 检查索引是否已存在
    if (tab.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }
    
    // 4. 获取列元数据
    std::vector<ColMeta> idx_cols;
    for (const auto &col_name : col_names) {
        auto col_iter = tab.get_col(col_name);
        if (col_iter == tab.cols.end()) {
            throw ColumnNotFoundError(col_name);
        }
        idx_cols.push_back(*col_iter);
    }
    
    try {
        // 1. 构造文件名
        std::string base_name = tab_name + ".0";
        std::string index_file_path = base_name + "_" + col_names[0] + ".idx";
        
        std::cout << "Creating index file: " << index_file_path << std::endl;
        
        // 2. 确保文件不存在
        if (disk_manager_->is_file(index_file_path)) {
            disk_manager_->destroy_file(index_file_path);
        }
        
        // 3. 创建文件
        disk_manager_->create_file(index_file_path);
        
        // 4. 打开文件获取文件描述符
        int fd = disk_manager_->open_file(index_file_path);
        
        // 5. 创建并初始化文件头
        char header_page[PAGE_SIZE] = {0};  // 初始化为0
        IxFileHdr file_hdr;
        file_hdr.root_page_ = INVALID_PAGE_ID;  // 初始化根页面ID
        file_hdr.num_pages_ = 1;  // 初始只有头页面
        file_hdr.tot_len_ = sizeof(IxFileHdr);  // 设置正确的总长度
        
        // 序列化文件头
        file_hdr.serialize(header_page);
        
        // 写入文件头页面
        disk_manager_->write_page(fd, 0, header_page, PAGE_SIZE);
        
        // 关闭文件以确保数据写入磁盘
        disk_manager_->close_file(fd);
        
        // 6. 现在使用 IxManager 创建索引
        ix_manager.create_index(index_file_path, idx_cols);
        
        // 7. 添加索引元数据
        IndexMeta idx_meta;
        idx_meta.col_num = col_names.size();
        idx_meta.cols = idx_cols;
        tab.indexes.push_back(idx_meta);
        
        // 8. 打开索引进行记录插入
        auto ix_handle = ix_manager.open_index(base_name, idx_cols);
        auto fh = fhs_.at(tab_name).get();
        
        // 9. 插入记录
        for (RmScan rmScan(fh); !rmScan.is_end(); rmScan.next()) {
            auto rid = rmScan.rid();
            auto record = fh->get_record(rid, context);
            
            // 构造索引键
            int total_len = 0;
            for (const auto &col : idx_cols) {
                total_len += col.len;
            }
            char *key = new char[total_len];
            int offset = 0;
            for (const auto &col : idx_cols) {
                memcpy(key + offset, record->data + col.offset, col.len);
                offset += col.len;
            }
            
            // 插入索引项
            ix_handle->insert_entry(key, rid, nullptr);
            delete[] key;
        }
        
    } catch (const std::exception &e) {
        std::cerr << "Error in create_index: " << e.what() << std::endl;
        throw;
    }
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    
}

