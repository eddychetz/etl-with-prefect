# etl-with-prefect

A data pipeline for daily data injestions

## STEPS:

- Install Prefect on a virtual environment `prefect-env` or activate by running `prefect-venv\Scripts\activate`

- Identify the path with the `python.exe` file by running `py -c "import sys; print(sys.executable)"` on the terminal.

- Create a virtual environment within your directory `etl-with-prefect` on GitHub.

- Run `New-PSDrive -Name prefect -PSProvider FileSystem -Root"C:\Users\Eddie\OneDrive - eRoute2Market\eRoute2Market\Agents\prefect\etl-with-prefect"` to create an alias for your path to `prefect:` on the terminal.

- Run `cd prefect:` to move to the alias path you created above.

- Run `py -m venv prefect-env` in a new terminal in the **prefect** directory.

## Deployment

1. Create a `prefect.yaml` file to schedule your deployment to run at 8:20 daily.
2. Create a new work-pool on Prefect Cloud and run `prefect deploy --all`
3. Follow the link shown on the terminal to view the deployment.
