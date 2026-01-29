# base Docker image that we build on
FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# set up working directory inside the container
WORKDIR /code

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/code/.venv/bin:$PATH"

# copy dependency files to working directory
COPY pyproject.toml .python-version uv.lock ./
# install dependencies from lock file
RUN uv sync --locked

# # copy the script into the container. 1st name is source file, 2nd is destination
# # dot means current working directory inside the image
COPY ingest_data.py ingest_data_2.py ingest_data_3.py .

ENTRYPOINT ["uv", "run", "python", "ingest_data_2.py"]
 