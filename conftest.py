import sys
from pathlib import Path

# 保证从仓库根目录可导入 common / diag / source 包
sys.path.insert(0, str(Path(__file__).resolve().parent))
