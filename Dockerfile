FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.9.22 /uv /uvx /bin/

#  Set a working directory
WORKDIR /skz_pace

# Copy dependency files first and README.me
COPY pyproject.toml .

# Install dependencies using uv. 
# We use '--system' because inside Docker, we don't need virtual environments! The container IS the environment.
RUN uv pip install --system -r pyproject.toml

# Copy the rest of the project files into the container
COPY . .

# Tell Docker what to do when the container starts
CMD ["python", "--version"]