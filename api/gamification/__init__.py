from fastapi import APIRouter

from .router_admin import router as admin_router
from .router_public import router as public_router

router = APIRouter()

router.include_router(admin_router)
router.include_router(public_router)