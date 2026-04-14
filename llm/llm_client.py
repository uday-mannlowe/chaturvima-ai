from groq import Groq
from dotenv import load_dotenv
from core.config import Config

load_dotenv()

client = Groq(
    api_key=Config.GROQ_API_KEY
)
