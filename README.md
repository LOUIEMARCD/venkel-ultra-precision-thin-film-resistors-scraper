# Venkel Ultra Precision Thin Film Resistors Scraper

Scrapes the complete product listing from:

`https://venkel.com/category/resistors/ultra-precision-thin-film-resistors?q=*`

The scraper runs in GitHub Actions using Playwright and headed Chromium. It saves resumable checkpoint data in `data/`, automatically resumes after an interrupted run, and uploads the final CSV as an Actions artifact.

Main output:

`data/venkel_ultra_precision_thin_film_resistors.csv`
