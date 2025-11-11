from fastapi import APIRouter

from .avatar_public import router as avatar_public_router
from .routes_me_account import router as routes_me_account_router
from .account_delete import router as delete_router

router= APIRouter()

router.include_router(avatar_public_router)
router.include_router(routes_me_account_router)
router.include_router(delete_router)
