from concurrent.futures import ThreadPoolExecutor
import json
import os
import pathlib
import ssl
import sys
import threading
import urllib.error
import urllib.request


BASE = os.environ["OPENAI_BASE_URL"].rstrip("/")
KEY = os.environ["OPENAI_API_KEY"]

CA = os.environ.get("PROXY_CA_CERT_PATH")
CTX = ssl.create_default_context(cafile=CA) if CA else None


def _atomic_write_text(path, value):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        tmp.write_text(value)
        os.replace(tmp, path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def ask(
    prompt,
    model="gpt-5.6-sol",
    effort="max",
    mode=None,
):
    reasoning = {
        "effort": effort,
    }

    if mode == "pro":
        reasoning["mode"] = "pro"

    body = {
        "model": model,
        "input": prompt,
        "reasoning": reasoning,
        "store": False,
        "service_tier": "fast"
    }

    request = urllib.request.Request(
        url=BASE + "/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": "Bearer " + KEY,
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, context=CTX) as response:
            raw_text = response.read().decode("utf-8")
            data = json.loads(raw_text)

    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HTTP {error.code} {error.reason}\n{error_body}"
        ) from error

    except urllib.error.URLError as error:
        raise RuntimeError(f"Connection error: {error.reason}") from error

    text_parts = []
    for output_item in data.get("output", []):
        if output_item.get("type") != "message":
            continue

        for content_item in output_item.get("content", []):
            if content_item.get("type") == "output_text":
                text = content_item.get("text", "")
                if text:
                    text_parts.append(text)

    return "\n".join(text_parts).strip()


def ask_many(prompts, output_paths=None, max_workers=None, **ask_options):
    """Run independent prompts concurrently and return results in input order.

    When output_paths is supplied, each text response is atomically written to
    its corresponding path. Distinct paths remain fully independent.
    """
    prompts = list(prompts)
    if output_paths is None:
        paths = [None] * len(prompts)
    else:
        paths = list(output_paths)
        if len(paths) != len(prompts):
            raise ValueError("output_paths must have one entry per prompt")
    if not prompts:
        return []

    def run_one(item):
        prompt, path = item
        result = ask(prompt, **ask_options)
        if path is not None:
            _atomic_write_text(path, result)
        return result

    workers = max_workers or min(32, len(prompts))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(run_one, zip(prompts, paths)))


if __name__ == "__main__":
    prompt = " ".join(sys.argv[1:]).strip() or "Say hi"

    # Standard mode:
    # print(ask(prompt))

    # Pro mode:
    print(
        ask(
            prompt,
            model="gpt-5.6-sol",
            effort="max",
            mode="pro",
        )
    )