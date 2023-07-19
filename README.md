# Selenium File Downloader

This script uses Selenium to automate file downloading from a website and then moves the downloaded file to a specified destination folder. It is specifically designed to download a CSV file from the FTSE website.

## Prerequisites

- Python 3.x
- Selenium library
- Chrome WebDriver

## Installation

1. Install Python from the [official Python website](https://www.python.org/downloads/) and follow the installation instructions.

2. Install the Selenium library by running the following command:
   
3. Download the Chrome WebDriver that matches your Chrome browser version. You can download it from the [ChromeDriver website](https://sites.google.com/a/chromium.org/chromedriver/downloads). Make sure to place the WebDriver executable in a directory that is included in your system's PATH environment variable.

## Usage

1. Open the script file `selenium_file_downloader.py` in a text editor.

2. Modify the following variables with the appropriate values:

- `login_url`: The URL of the login page on the FTSE website.
- `username`: Your FTSE account username.
- `password`: Your FTSE account password.
- `download_folder`: The path to the folder where the files will be downloaded.
- `destination_folder`: The path to the destination folder where the downloaded file will be moved.
- `source_file_name`: The name of the downloaded file on the FTSE website.

3. Save the modified script file.

4. Run the script by executing the following command in the terminal:

5. The script will open a Chrome browser window, navigate to the login page, enter your credentials, and log in to the FTSE website. It will then navigate to the specific folder and download the CSV file. After the download is complete, the script will rename the file with the current date and move it to the destination folder.

6. The downloaded file will be saved with a name in the format `TWII-YYYY-MM-DD.csv`, where `YYYY-MM-DD` represents the current date.

Note: Make sure the Chrome browser is installed on your system, and the Chrome WebDriver is compatible with the installed Chrome version.

That's it! You can now use this script to automate the file downloading process from the FTSE website usin
