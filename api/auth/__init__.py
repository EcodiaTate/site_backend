from fastapi import APIRouter

from .logout import router as logout_router
from .sso_login import router as sso_router
from .set_role import router as set_role_router
from .auth_main import router as main_router
from .me import router as me_router
from .refresh import router as refresh_router

router = APIRouter()

router.include_router(logout_router)
router.include_router(sso_router)
router.include_router(set_role_router)
router.include_router(main_router)
router.include_router(me_router)
router.include_router(refresh_router)

__all__ = ["set_role_router"]
