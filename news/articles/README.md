## Newspaper

The *newspaper* folder contains a subtree with the [Newspaper](https://github.com/codelucas/newspaper) library, which we have modified to accommodate pre-pending a proxy string from [Scraper API](https://www.scraperapi.com/) when necessary. We also changed the bot's user agent string to identify as Code for Democracy.

The **newspaper** library needs to be packaged with **news_articles_ingest_get_paper**, **news_articles_ingest_get_articles**, and **news_articles_ingest_get_url**. Basically, this means that we need to copy the *newspaper/newspaper* folder into the *news_articles_ingest_get_paper* or *news_articles_ingest_get_articles* folders before deploying. We should also check the *requirements.txt* to make sure that the requirements from **newspaper** are updated.
