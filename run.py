import subprocess
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent
app_file = project_root / "web" / "app.py"
venv_windows_python = project_root / ".venv" / "Scripts" / "python.exe"
venv_unix_python = project_root / ".venv" / "bin" / "python"

python_exec = sys.executable
if sys.platform.startswith("win") and venv_windows_python.exists():
    python_exec = str(venv_windows_python)
elif not sys.platform.startswith("win") and venv_unix_python.exists():
    python_exec = str(venv_unix_python)

try:
    cmd = [
        python_exec,
        "-m",
        "streamlit",
        "run",
        str(app_file),
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
    ]
    print(f"使用解释器: {python_exec}", flush=True)
    print("正在启动 Streamlit，请稍候...", flush=True)
    print("启动后请访问: http://localhost:8501", flush=True)
    subprocess.run(
        cmd,
        check=True,
    )
except KeyboardInterrupt:
    print("\n已停止服务。", flush=True)
except (OSError, subprocess.CalledProcessError) as error:
    print(f"启动失败: {error}")
    print("请先安装依赖: python -m pip install -r requirements.txt")
    raise