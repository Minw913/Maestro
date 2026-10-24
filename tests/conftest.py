from pathlib import Path

from dotenv import load_dotenv

_ = load_dotenv(Path(__file__).resolve().parents[1] / ".env")
