import docker
from docker.errors import ImageNotFound, DockerException

# # 方法1: 自动检测连接方式（默认）
# client = docker.from_env()

# 方法2: 指定连接参数
client = docker.DockerClient(
    # base_url='unix://var/run/docker.sock',  # Unix socket
    # base_url='tcp://localhost:2375',      # TCP连接
    base_url='npipe:////./pipe/docker_engine',  # Windows named pipe
    timeout=10,
    version='auto'  # 自动检测API版本
)

# # 方法3: 使用环境变量
# import os
# os.environ['DOCKER_HOST'] = 'tcp://localhost:2375'
# os.environ['DOCKER_TLS_VERIFY'] = '0'
# client = docker.from_env()

# 测试连接
try:
    print("Docker 版本:", client.version())
    print(client.api.base_url)
    print("连接成功!")
except DockerException as e:
    print("连接失败:", e)


def pull_image(image_name, tag='latest'):
    """拉取 Docker 镜像"""
    try:
        # 检查本地是否已存在
        client.images.get(f"{image_name}:{tag}")
        print(f"镜像 {image_name}:{tag} 已存在")
        return True
    except ImageNotFound:
        try:
            print(f"正在拉取镜像 {image_name}:{tag}...")
            # 显示拉取进度
            image = client.images.pull(
                repository=image_name,
                tag=tag
            )
            print(f"镜像拉取成功: {image.tags}")
            return True
        except docker.errors.APIError as e:
            print(f"拉取镜像失败: {e}")
            return False


# 示例：拉取常用镜像
images_to_pull = [
    ("hello-world", "latest")
]

for img, tag in images_to_pull:
    pull_image(img, tag)


def list_images():
    """列出所有镜像"""
    images = client.images.list()

    print(f"{'镜像ID':<15} {'标签':<40} {'大小':<15} {'创建时间':<20}")
    print("-" * 100)

    for image in images:
        image_id = image.id[:12] if image.id else 'N/A'
        tags = ', '.join(image.tags[:3]) if image.tags else '<none>'
        if len(image.tags) > 3:
            tags += '...'
        size = f"{image.attrs['Size'] / (1024 * 1024):.2f} MB"
        created = image.attrs['Created'][:19]  # 截取日期部分

        print(f"{image_id:<15} {tags:<40} {size:<15} {created:<20}")


list_images()
