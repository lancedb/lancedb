import os
from typing import Optional
from pydantic import BaseModel
from functools import cached_property
from ...util import attempt_import_or_raise
from ..utils import api_key_not_found_help


class BaseLLM(BaseModel):
    """
    TODO:
    Base class for Language Model based Embedding Functions. This class is 
    loosely desined rn, and will be updated as the usage gets clearer.
    """
    model_name: str
    model_kwargs: dict = {}

    @cached_property
    def _client():
        """
        Get the client for the language model
        """
        raise NotImplementedError
    
    def chat_completion(self, prompt: str, **kwargs):
        """
        Get the chat completion for the given prompt
        """
        raise NotImplementedError

class Openai(BaseLLM):
    model_name: str = "gpt-3.5-turbo"
    kwargs: dict = {}
    api_key: Optional[str] = None

    @cached_property
    def _client(self):
        """
        Get the client for the language model
        """
        openai = attempt_import_or_raise("openai")

        if not os.environ.get("OPENAI_API_KEY"):
            api_key_not_found_help("openai")
        return openai.OpenAI()

    def chat_completion(self, prompt: str) -> str:
        """
        Get the chat completion for the given prompt
        """

        # TODO: this is legacy openai api replace with completions
        completion = self._client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt},
            ],
            **self.kwargs
            )

        text = completion.choices[0].message.content

        return text
    
class Gemini(BaseLLM):
    pass