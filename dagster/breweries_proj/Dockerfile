FROM python:3.10

# Creating folder to import the project
ENV DAGSTER_HOME="/dagster_home"
RUN mkdir -p $DAGSTER_HOME
WORKDIR $DAGSTER_HOME

# Delta Lake Path
ENV STORAGE_PATH=$DAGSTER_HOME/deltalake

# Copying files
COPY . $DAGSTER_HOME/

# Installing Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN ~/.local/share/pypoetry/venv/bin/poetry config virtualenvs.create false
RUN ~/.local/share/pypoetry/venv/bin/poetry install --only main

# Dagster UI
EXPOSE 3000

# Initiating Dagster
ENTRYPOINT dagster dev