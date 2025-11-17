from fastapi import APIRouter

from .admin_users import router as users_router
from .radical_transparency import router as rad_tra_router


router = APIRouter()

router.include_router(users_router)
router.include_router(rad_tra_router)

