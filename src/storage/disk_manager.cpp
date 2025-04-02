/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <cassert>  // 添加cassert头文件，支持assert断言
#include <unordered_map>
#include <string>
#include "common/config.h"

#include "defs.h"

static const int MAX_FD = 1024;  // 最大文件描述符数量

DiskManager::DiskManager() {
    memset(fd2pageno_, 0, MAX_FD * sizeof(page_id_t));
    
    // 初始化文件描述符占用表
    for (int fd = 0; fd < MAX_FD; fd++) {
        fd_occupied_[fd] = false;
    }
}

/**
 * @description: 将数据写入到磁盘中的指定页面
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面号
 * @param {char} *offset 要写入的数据
 * @param {int} num_bytes 要写入的数据大小
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用write()函数
    // 注意write返回值与num_bytes不等时 throw InternalError("DiskManager::write_page Error");
    
    // 计算页面在文件中的偏移位置
    off_t page_offset = static_cast<off_t>(page_no) * PAGE_SIZE;
    
    // 定位到文件中的正确位置
    if (lseek(fd, page_offset, SEEK_SET) == -1) {
        throw InternalError("DiskManager::write_page lseek failed: " + std::string(strerror(errno)));
    }
    
    // 写入数据
    ssize_t bytes_written = write(fd, offset, num_bytes);
    
    // 检查写入是否完整
    if (bytes_written != num_bytes) {
        throw InternalError("DiskManager::write_page Error");
    }
    
    // 确保数据持久化到磁盘
    fsync(fd);
}

/**
 * @description: 读取磁盘中指定页面的数据
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面号
 * @param {char} *offset 读取的数据输出位置
 * @param {int} num_bytes 要读取的数据大小

  // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用read()函数
    // 注意read返回值与num_bytes不等时，throw InternalError("DiskManager::read_page Error");
    
    // 计算页面在文件中的偏移位置
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    // 计算页面在文件中的偏移位置
    off_t page_offset = static_cast<off_t>(page_no) * PAGE_SIZE;
    
    // 获取文件大小以进行检查
    struct stat file_stat;
    if (fstat(fd, &file_stat) == -1) {
        throw InternalError("DiskManager::read_page fstat failed: " + std::string(strerror(errno)));
    }
    
    // 如果尝试读取的位置超出文件大小，直接返回零填充
    if (page_offset >= file_stat.st_size) {
        memset(offset, 0, num_bytes);
        return;  // 文件未初始化到这个位置，返回零页面
    }
    
    // 定位到文件中的正确位置
    if (lseek(fd, page_offset, SEEK_SET) == -1) {
        throw InternalError("DiskManager::read_page lseek failed: " + std::string(strerror(errno)));
    }
    
    // 读取数据
    ssize_t bytes_read = read(fd, offset, num_bytes);
    
    // 检查读取是否完整
    if (bytes_read != num_bytes) {
        // 提供更详细的错误信息
        std::string error_msg = "DiskManager::read_page Error - ";
        if (bytes_read == -1) {
            error_msg += "Read failed: " + std::string(strerror(errno));
        } else {
            error_msg += "Incomplete read: requested " + std::to_string(num_bytes) + 
                       " bytes, got " + std::to_string(bytes_read) + " bytes";
        }
        throw InternalError(error_msg);
    }
}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    // 简单的自增分配策略，指定文件的页面编号加1
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 * @return {bool} 若指定路径文件存在则返回true 
 * @param {string} &path 文件路径
 */
bool DiskManager::is_file(const std::string &path) {
    // Todo:
    // 用struct stat获取文件信息
    
    struct stat st;
    // stat函数返回0表示成功获取文件信息，表示文件存在
    return stat(path.c_str(), &st) == 0;
}

/**
 * @description: 创建指定路径文件
 * @param {string} &path 文件路径
 */
void DiskManager::create_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_CREAT模式
    // 注意不能重复创建相同文件
    
    // 检查文件是否已存在
    if (is_file(path)) {
        throw FileExistsError(path);
    }
    
    // 创建新文件
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    if (fd == -1) {
        throw UnixError(); // 不传入参数，构造函数会自动获取errno
    }
    
    // 创建后立即关闭文件
    close(fd);
}

/**
 * @description: 删除指定路径文件
 * @param {string} &path 文件路径
 */
void DiskManager::destroy_file(const std::string &path) {
    // Todo:
    // 调用unlink()函数
    // 注意不能删除未关闭的文件
    
    // 检查文件是否存在
    if (!is_file(path)) {
        throw FileNotFoundError(path);
    }
    
    // 检查文件是否已经打开
    for (int fd = 0; fd < MAX_FD; fd++) {
        if (fd_occupied_[fd] && fd2path_[fd] == path) {
            throw InternalError("Cannot destroy opened file");
        }
    }
    
    // 删除文件
    if (unlink(path.c_str()) == -1) {
        throw UnixError(); // 不传入参数，构造函数会自动获取errno
    }
}

/**
 * @description: 打开指定路径文件
 * @return {int} 打开的文件的文件句柄
 * @param {string} &path 文件路径
 */
int DiskManager::open_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_RDWR模式
    // 注意不能重复打开相同文件，并且需要更新文件打开列表
    
    // 检查文件是否存在
    if (!is_file(path)) {
        throw FileNotFoundError(path);
    }
    
    // 以读写模式打开文件
    int fd = open(path.c_str(), O_RDWR);
    if (fd == -1) {
        throw UnixError(); // 不传入参数，构造函数会自动获取errno
    }
    
    // 检查文件描述符范围
    if (fd >= MAX_FD) {
        close(fd);
        throw InternalError("Too many files opened");
    }
    
    // 检查文件是否已打开
    if (fd_occupied_[fd]) {
        close(fd);
        throw InternalError("File descriptor already occupied");
    }
    
    // 更新文件打开状态
    fd_occupied_[fd] = true;
    fd2path_[fd] = path;
    
    return fd;
}

/**
 * @description: 关闭指定路径文件
 * @param {int} fd 文件句柄
 */
void DiskManager::close_file(int fd) {
    // Todo:
    // 调用close()函数
    // 注意不能关闭未打开的文件，并且需要更新文件打开列表
    
    // 检查文件描述符有效性
    if (fd < 0 || fd >= MAX_FD) {
        throw InternalError("Invalid file descriptor");
    }
    
    // 检查文件是否已打开
    if (!fd_occupied_[fd]) {
        throw InternalError("File not opened");
    }
    
    // 关闭文件
    if (close(fd) == -1) {
        throw UnixError(); // 不传入参数，构造函数会自动获取errno
    }
    
    // 更新文件状态
    fd_occupied_[fd] = false;
    fd2path_.erase(fd);
}

/**
 * @description: 获得文件的大小
 * @return {int} 文件的大小
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_size(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager::get_file_name(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int DiskManager::read_log(char *log_data, int size, int offset) {
    // read log file from the previous end
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    int file_size = get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if(size == 0) return 0;
    lseek(log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_fd_, log_data, size);
    assert(bytes_read == size);
    return bytes_read;
}

/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    // write from the file_end
    lseek(log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_fd_, log_data, size);
    if (bytes_write != size) {
        throw UnixError();
    }
}