from fastapi import APIRouter

from .apply import router as apply_router
from .my_store import router as my_store_router
from .catalogue import router as catalogue_router
from .checkout import router as checkout_router
from .store_settings import router as settings_router
from .payouts import router as payouts_router
from .connect import router as connect_router
from .my_orders import router as my_orders_router
from .admin import router as admin_router


router= APIRouter()

router.include_router(apply_router)
router.include_router(my_store_router)
router.include_router(catalogue_router)
router.include_router(settings_router)
router.include_router(checkout_router)
router.include_router(payouts_router)
router.include_router(connect_router)
router.include_router(my_orders_router)
router.include_router(admin_router)
