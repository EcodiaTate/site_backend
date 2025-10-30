from fastapi import APIRouter

from .admin_submissions import router as admin_router
from .routers import router as public_router, media_router

router = APIRouter()

router.include_router(admin_router)
router.include_router(public_router)
router.include_router(media_router)
