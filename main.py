# Install Prefect ----
# * pip install prefect
# * prefect cloud login and choose to login with a web browser

# * run `prefect deploy etl-with-prefect/main.py:main_flow`
# * run `prefect dashboard`



# 1.0 Libraries ----
from prefect import flow, task#, get_run_logger
from prefect.blocks.system import JSON

# 2.0 Extract ----
@task
def extract():
    print('Hello, step 1 is running!!')

# 3.0 Transform ----
@task
def transform():
    print('Hello, step 2 is running!!')

# 4.0 Load ----
@task
def load():
    print('Hello, step 3 is running!!')

# 5.0 Main Flow ----
@flow(name='ETL Workflow', log_prints=True)
def main_flow():
    step1 = extract()
    print(step1)
    step2 = transform()
    print(step2)
    step3 = load()
    print(step3)

# 6.0 Run ----
if __name__ == '__main__':
    main_flow()


# 7.0 Deployment
# * prefect deploy [OPTIONS] [ENTRYPOINT] e.g ./etl-with-prefect/main.py:main_flow
# * run `prefect deploy -name test-deployment ./etl-with-prefect/main.py:main_flow`
# * run `prefect dashboard`