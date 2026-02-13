import sys
import os

# 处理内部依赖路径
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ib_async_path = os.path.join(base_dir, 'ib_async')
lc_path = os.path.join(base_dir, 'lightweight-charts-python')

if ib_async_path not in sys.path:
    sys.path.insert(0, ib_async_path)
if lc_path not in sys.path:
    sys.path.insert(0, lc_path)

from .collector import Collector
from .viewer import Viewer

__all__ = ['Collector', 'Viewer']
