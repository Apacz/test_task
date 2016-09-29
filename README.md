Developer Test Application
========================

This is originally a fork of the "Symfony Demo Application", this test can be run entirely on the command line
and thus no webserver is needed.  The application comes with a sqlite database, however you can replace this
with another DB if it suits your needs.

Please note, this code may have many unneccesary files/variables as it is just for testing purposes :)

Requirements
------------

  * PHP 5.5.9 or higher;
  * PDO-SQLite PHP extension enabled;
  * and the [usual Symfony application requirements](http://symfony.com/doc/current/reference/requirements.html).


Installation
------------


> If you can't use the Symfony Installer, download and install the demo
> application using Git and Composer:
>
>     $ git clone https://github.com/zoborg/symfony-demo
>     $ cd symfony-demo/
>     $ composer install --no-interaction

When you have read below you should execute below (this may take some time to load).

>     $ ./bin/console app:loadreport

The Challenge
-----

This is typical data challenge we need to solve within our application, we process large datasets and will then present
a summary view to the user.  Often this will use aggregate functions (E.g. sum) to create these summary views, however
with larger accounts, or as the data grows over time these aggregate views become inefficient and a alternative needs to
be found.

In a typical PPC Campaign you will have a structure such as follows

   Campaign
     Adgroup 1 (Many to 1 relationship with a Campaign)
         Keywords (Many to 1 relationship to a Adgroup)
            Keyword 1
            Keyword 2
            Keyword 3
         SKU (e.g. a particular product) (Many to 1 relationship with a adgroup)
            Mobile Phone Case
            Other Mobile Phone Case
      Adgroup 2
         Keywords
            Keyword 1
            Keyword 2
            Keyword 3
         SKU (e.g. a particular product)
            Mobile Phone Case
            Other Mobile Phone Case

We will get reports similar to this (To simplify we are only using clicks/impressions, extra data in the test file
can be ignored)  :~

Example 1.

| Campaign Name |	Ad Group Name	| Advertised SKU	| Keyword	 | Match Type	| Start Date	| End Date | Clicks |  Impressions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
|Mobile Phone  |  Adroup 1    |    Iphone   |       case   | broad|       2016-09-01|  2016-09-02|  1|       100|
|Mobile Phone   | Adroup 1     |   Iphone    |      phone  | broad|       2016-09-01|  2016-09-02|  2|       200|
|Mobile Phone   | Adroup 1     |   Iphone    |      black  | broad|       2016-09-01|  2016-09-02|  3|       300|
|Mobile Phone   | Adroup 1      |  Iphone     |     green  | exact|       2016-09-01|  2016-09-02|  4|       400|

** Please note, in the example file all above is replaced with md5 hashes for security reasons.


Taking the above example data, we would wish to provide aggregate views of the data, e.g. a sum of all the clicks
between X and Y date, grouped by the SKU.  Or give me the sum of clicks, impressions grouped by the day of month.  What you
will notice in almost all cases the KEYWORD/MATCHTYPE is not relevant to the group.  E.g. if we ignored the keyword & matchtype
columns, the aggregated data could be.

Example 2.

| Campaign Name |	Ad Group Name	| Advertised SKU	| Start Date	| End Date | Clicks |  Impressions |
| --- | --- | --- | --- | --- | --- | --- |
|Mobile Phone|    Adroup 1|        Iphone|          2016-09-01|  2016-09-02|  10|       1000|

If we were to query this summary view of the data, we could achieve the same results as querying the full data
however significantly less rows would need to scanned.

Included in this test is 2 command's

1)  LoadReportCommand (run as ./bin/console app:loadreport), this command will load the testdata into the database.  This
test data is stored under var/data/testdata.txt and contains approximately 115000 rows of data, a typical large account
where the data would be updated twice a day.

When you run this command you will notice it calls the AppBundle:Utils:CampaignPerformanceProcessor  to load
this data in as efficient way as possible.  In a real environment this is done via a queue and as such has no impact to
the customer, therefore any increased latency to this process is quite acceptable.

2)  TestQueryCommand (run as ./bin/console app:testquery) - this command simulates a typical SQL (via Doctrine) lookup
that a user would trigger whilst using our app online.  Microtime has been put around the functions to estimate how long
the queries can take.  As you will see when running the command, with 115K records this querying can take 5-10 seconds
which is too slow to run online. (example output below).

>   $ Campaign Performance took 2.1192889213562 to load 4306 records
>   $ Ad Group Stats took 0.7291362285614 to load 709 records
>   $ Campaign Stats took 0.70429015159607 to load 358 records
>   $ Sku Stats took 0.57318997383118 to load 123 records
>   $ Total : 4.1263418197632

What I would like you to do is find a way to get the same results from 2), however significantly improve the time it takes
to return the SAME results.  One suggested way this could be achieved (or welcome to come up with other solutions) is,
by creating some summary view of the data,  e.g. as keyword/matchtype is not relevant.  This could be a database view
(although would need to swap sqlite out with another database), or create a table to hold this summary view.  How/
when this other table would then be populated is up to you.

Finally you would create alternatives to the methods in the CampaignPerformanceRepository, which could be called by the
test script to show improvements.  Please make sure you confirm that the data of the improved method is the same as the
original.

This test is to simulate a typical problem in a typical working relationship, as you would if we worked together feel
free to ask questions.
