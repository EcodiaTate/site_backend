from fastapi import APIRouter

from .admin_users import router as users_router


router = APIRouter()

router.include_router(users_router)

