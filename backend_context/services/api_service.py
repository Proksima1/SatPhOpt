import logging
import uuid
from dataclasses import dataclass
from urllib.parse import urljoin

from fastapi import UploadFile
from faststream.rabbit import RabbitBroker

from backend_context.persistent.pg.api import ImageProcessing
from backend_context.repositories.api_repository import ApiRepository
from backend_context.repositories.s3_repository import S3Repository
from backend_context.schemas.api_schemas import PresignedUrl, Task
from backend_context.suppliers.s3_supplier import S3Supplier
from base.presentation.rabbit.rabbit_queues import process_image_queue
from base.settings import settings


@dataclass(slots=True, frozen=True)
class ApiService:
    _s3_supplier: S3Supplier
    _api_repository: ApiRepository
    _s3_repository: S3Repository
    _publisher: RabbitBroker

    async def get_presigned_url(self) -> PresignedUrl:
        task_id = uuid.uuid4()
        url = await self._s3_supplier.get_presigned_url(task_id)
        await self._api_repository.add_presigned_url(url)
        return url

    async def get_image_by_task_id(self, task_id: uuid.UUID) -> Task:
        task = await self._api_repository.get_task_by_id(task_id)
        if task.status == ImageProcessing.READY.value:
            task.s3_url = await self._make_url_to_image(task_id)
        return task

    async def upload_satellite_images(self, optical_file: UploadFile, sar_file: UploadFile) -> Task:
        task_id = uuid.uuid4()
        await self._api_repository.add_task(task_id=task_id)
        optical_content, sar_content = await optical_file.read(), await sar_file.read()
        await self._s3_repository.upload_images(
            task_id=task_id, optical_content=optical_content, sar_content=sar_content
        )
        await self._send_task_in_queue(task_id=task_id)
        await optical_file.close()
        await sar_file.close()
        return Task(task_id=task_id, status=ImageProcessing.QUEUED, s3_url=None)

    async def _send_task_in_queue(self, task_id: uuid.UUID) -> None:
        await self._publisher.publish(str(task_id), queue=process_image_queue)
        logging.info("task.queued", extra={"task_id": task_id})

    async def _make_url_to_image(self, task_id: uuid.UUID) -> str:
        base_url = urljoin(
            urljoin(settings.s3_config.endpoint_url, settings.s3_config.bucket_name), "testka/testka/uploads/"
        )
        return urljoin(base_url, str(task_id)) + "/result.tif"
