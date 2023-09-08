# YouTube Data Harvesting and Warehousing


<img src="https://upload.wikimedia.org/wikipedia/commons/e/ef/Youtube_logo.png" alt="YouTube Logo" width="200" height="139">



## Overview

This GitHub project, "YouTube Data Harvesting and Warehousing," is a comprehensive solution for collecting, storing, and analyzing data from YouTube. Whether you're a data scientist, marketer, or just curious about YouTube trends, this project provides the tools and resources you need to harvest and warehouse YouTube data efficiently.

## Table of Contents

- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Data Harvesting](#data-harvesting)
- [Data Warehousing](#data-warehousing)
- [Analysis and Visualization](#analysis-and-visualization)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

This section provides an overview of the project and instructions on how to get started.

## Note [Ensure downloading below software if you use vscode already with older python

- [Anaconda Navigator](https://www.anaconda.com/installation-success)
- [install latest python version -3.11](https://www.python.org/downloads/release/python-3110/)
- [Make sure your drive directory doesn't inculde a space "  " character in it]

### Prerequisites

Before you begin, make sure you have the following prerequisites installed:

- Python 3.6+
- Git
- MySQL or PostgreSQL (for data warehousing)
- Google API credentials (for YouTube data harvesting)

### Installation

1. Clone this repository to your local machine:

```bash
git clone https://github.com/Benny-752/youtube-data-harvesting-and-warehousing.git
cd youtube-data-harvesting-and-warehousing
```

2. Install the required Python packages:

```bash
pip install -r requirements.txt
```

3. Set up your database (MySQL or PostgreSQL) and configure the database connection in the `config.py` file.

4. Obtain Google API credentials by following the instructions in the [Google API Documentation](https://developers.google.com/youtube/registering_an_application).

5. Configure your Google API credentials in the `config.py` file.

## Usage

This section describes how to use the project for YouTube data harvesting and warehousing.

### Data Harvesting

To harvest data from YouTube, run the following command:

```bash
python harvest_data.py
```

This script will fetch the latest YouTube videos, comments, and other relevant data based on your specified search criteria and store it in your database.

### Data Warehousing

To store and warehouse the harvested data in your database, run the following command:

```bash
python warehouse_data.py
```

This script will organize and store the collected data in your MySQL or PostgreSQL database for further analysis.

### Analysis and Visualization

You can now use your preferred data analysis and visualization tools to analyze and visualize the YouTube data stored in your database.

## Contributing

Contributions to this project are welcome! If you have ideas, bug fixes, or new features to add, please submit a pull request following our [contributing guidelines](CONTRIBUTING.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Special thanks to the YouTube Data API for providing access to valuable YouTube data.
- Thanks to the open-source community for creating and maintaining the libraries and tools used in this project.

Happy data harvesting and warehousing!
