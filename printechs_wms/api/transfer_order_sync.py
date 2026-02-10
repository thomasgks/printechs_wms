# -*- coding: utf-8 -*-
"""
Transfer Order sync for WMS Desktop. get_tos_for_wms is in wms_sync; re-export here for backwards compatibility.
"""
from __future__ import annotations

from .wms_sync import get_tos_for_wms  # noqa: F401

__all__ = ["get_tos_for_wms"]
