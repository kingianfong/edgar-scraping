# edgar-scraping
This script was written to go through EDGAR 10-K files to find keywords within the "Risk Factors" section.

This was written before I had knowledge of regular expressions, or web scraping libraries, and before learning programming in a formal setting.

I tried to infer the end of "Risk Factors" by using the second occurrence of "Unresolved Staff Comments" as it is always the section that follows in a 10-K file. Unfortunately, "Risk Factors" is a common phrase which could appear anywhere in a filing, and the HTML varies between different files. To get a more accurate results, I used the strings before the second (first occurrence is usually contents' page) of "Unresolved Staff Comments" to infer the HTML tags which correspond to section headers. The final output includes counts using both methods.

For multithreading, locks were used and delays were added between requests (exceeding 10 requests/second results in a ban).
