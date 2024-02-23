import hashlib
import requests


def network_file_info(url):
    """网络文件相关信息"""

    response = requests.get(url)
    content = response.content
    hash_md5 = hashlib.md5(content).hexdigest()
    file_size = int(response.headers.get('Content-Length', 0))
    r = {'md5': hash_md5, 'length': file_size}
    return r
