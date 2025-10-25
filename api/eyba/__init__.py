from fastapi import APIRouter

from .business import router as b_router
from .places import router as p_router
from .claims import router as c_router
from .billing import router as billing_router
from .offers import router as offers_router
from .assets import router as assets_router
from .business_stats import router as business_stats_router
from .wallet import router as wallet_router
from .onboard import router as onboarding_router
from .eyba_business_public import router as bizz_public_router
from .youth_stats import public_router as youth_stats_router, admin_router as admin_youth_stats_router

router= APIRouter()

router.include_router(b_router)
router.include_router(p_router)
router.include_router(c_router)
router.include_router(billing_router)
router.include_router(offers_router)
router.include_router(assets_router)
router.include_router(business_stats_router)
router.include_router(youth_stats_router)
router.include_router(admin_youth_stats_router)
router.include_router(wallet_router)
router.include_router(bizz_public_router)
router.include_router(onboarding_router)