import hashlib
import logging
from pathlib import Path

from obs import ObsClient

from deepdiver_v2.config.config import get_obs_config

logger = logging.getLogger(__name__)


class OBSManager:
    """华为云 OBS 管理器"""

    def __init__(self):
        """
        初始化 OBS 客户端

        Args:
            access_key: 华为云 Access Key
            secret_key: 华为云 Secret Key
            server: OBS 服务端点，如 "https://obs.cn-north-4.myhuaweicloud.com"
            bucket: 存储桶名称
        """
        obs_config = get_obs_config()
        self.bucket = obs_config.get("obs_bucket")
        self.client = ObsClient(
            access_key_id=obs_config.get("obs_access_key"),
            secret_access_key=obs_config.get("obs_secret_key"),
            server=obs_config.get("obs_server")
        )

    def download_file(self, object_key: str = None, path: str = None):
        """
        从 OBS 下载文件
        
        Args:
            object_key: OBS 对象键
            path: 本地保存路径
            
        Returns:
            bool: 下载是否成功
        """
        try:
            resp = self.client.getObject(self.bucket, object_key, path)
            if resp.status < 300:
                logger.debug(f"[OBS] Successfully downloaded {object_key} to {path}")
                return True
            else:
                logger.error(f"[OBS] Download failed: {object_key}, status={resp.status}, reason={getattr(resp, 'reason', 'Unknown')}")
                return False
        except Exception as e:
            logger.error(f"[OBS] Download exception for {object_key}: {type(e).__name__}: {str(e)}")
            return False

    def upload_file(self, local_path: str, object_key: str = None,
                    public_read: bool = True) -> str:
        """
        上传文件到 OBS

        Args:
            local_path: 本地文件路径
            object_key: OBS 中的对象键（路径），如 "images/doc1/photo.png"
            public_read: 是否设置公共读权限

        Returns:
            (是否成功, 访问URL或错误信息)
        """
        if not object_key:
            # 使用文件哈希作为唯一键
            file_hash = self._calc_file_hash(local_path)
            ext = Path(local_path).suffix
            object_key = f"uploads/{file_hash[:16]}{ext}"

        try:
            # 上传文件
            resp = self.client.uploadFile(
                bucketName=self.bucket,
                objectKey=object_key,
                uploadFile=local_path,
                partSize=10 * 1024 * 1024,  # 10MB 分片
                taskNum=5,
                enableCheckpoint=True
            )

            if resp.status < 300:
                # 设置公共读权限（如果需要）
                if public_read:
                    self.client.setObjectAcl(
                        bucketName=self.bucket,
                        objectKey=object_key,
                        acl='public-read'
                    )

                # 生成访问 URL
                url = f"{object_key}"
                return url
            else:
                return ""

        except Exception as e:
            return ""

    def _calc_file_hash(self, file_path: str) -> str:
        """计算文件 MD5 哈希"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def close(self):
        """关闭客户端"""
        self.client.close()


obs = OBSManager()
