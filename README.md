# etl-with-prefect

A data pipeline for daily data injestions

STEPS:

Step 1: Install Prefect on virtual environment `prefect-env`

Identify path with `python.exe` file by running `py -c "import sys; print(sys.executable)"` on terminal.

Create virtual environment within **prefect** directory `etl-with-prefect` sitting on github.

Run `New-PSDrive -Name prefect -PSProvider FileSystem -Root"C:\Users\Eddie\OneDrive - eRoute2Market\eRoute2Market\Agents\prefect\etl-with-prefect"` to create alias of the path to `prefect:` on terminal.

Run `cd prefect:` to move to the alias path you created above.

Run `python3 -m venv prefect-env` on visula studio code on a new terminal in **prefect** directory.
