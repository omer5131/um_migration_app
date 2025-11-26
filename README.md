# Account Migration Recommendation Tool

This application automates the mapping of existing customer accounts to a new "Plan + Add-ons" pricing structure.

## Setup & Installation

1.  **Install Dependencies:**
    Ensure you have Python installed, then run:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Data Files:**
    Place all your CSV/Excel source files in the root directory of this project. The filenames are configured in `src/config.py`.

3.  **Running the App:**
    Run the application using Streamlit:
    ```bash
    streamlit run app.py
    ```

## Project Structure

* `app.py`: Main application entry point and UI.
* `src/`: Core logic and modules.
    * `config.py`: File paths and configuration constants.
    * `data_loader.py`: Loads CSV/Excel files into Pandas DataFrames.
    * `engine.py`: Contains the core recommendation algorithm.
    * `utils.py`: Helper functions for parsing and data cleaning.

