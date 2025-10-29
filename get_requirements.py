import subprocess
import re

# 指定要筛选的包名
packages_to_filter = [
    "aiohappyeyeballs",
    "aiohttp",
    "aiosignal",
    "async-timeout",
    "attrs",
    "blinker",
    "certifi",
    "charset-normalizer",
    "click",
    "colorama",
    "Columnar",
    "executing",
    "Flask",
    "Flask-Cors",
    "frozenlist",
    "gunicorn",
    "idna",
    "importlib_metadata",
    "itsdangerous",
    "Jinja2",
    "jsonify",
    "loguru",
    "MarkupSafe",
    "multidict",
    "numpy",
    "packaging",
    "pandas",
    "propcache",
    "python-dateutil",
    "pythonds",
    "pytz",
    "PyYAML",
    "requests",
    "six",
    "timeago",
    "toolz",
    "typing_extensions",
    "tzdata",
    "urllib3",
    "wcwidth",
    "Werkzeug",
    "win32_setctime",
    "yarl",
    "zipp",
    "gevent",
    "setuptools"
]

# 执行 pip list 命令并获取输出
result = subprocess.run(['pip', 'list'], capture_output=True, text=True)
output = result.stdout

# 解析输出，提取包名和版本
package_versions = {}
for line in output.splitlines():
    parts = line.split()
    if len(parts) == 2:
        package_name, package_version = parts
        package_versions[package_name] = package_version

# 筛选出指定的包
filtered_packages = {pkg: package_versions.get(pkg) for pkg in packages_to_filter if pkg in package_versions}

# 将结果保存到 result.txt 文件
with open('result.txt', 'w') as f:
    for pkg, version in filtered_packages.items():
        f.write(f"{pkg}=={version}\n")

print("结果已保存到 result.txt 文件中")