import os

# 定义根目录（处理波浪号~为实际用户目录）
root_dir = os.path.expanduser("~/.cache/aisbench")

# 存储所有目录的路径
all_dirs = []

# 递归遍历目录树
for dir_path, dir_names, file_names in os.walk(root_dir):
    # dirpath 就是当前遍历到的目录路径，直接加入列表
    all_dirs.append(dir_path)

# 打印所有目录（按层级顺序）
print("所有目录路径：")
for dir_path in all_dirs:
    print(dir_path)
