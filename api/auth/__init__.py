from fastapi import APIRouter

from .logout import router as logout_router
from .sso_login import router as sso_router
from .main import router as main_router
from .set_role import router as set_role_router

router = APIRouter(prefix="/auth")

router.include_router(logout_router)
router.include_router(sso_router)
router.include_router(main_router)
router.include_router(set_role_router)