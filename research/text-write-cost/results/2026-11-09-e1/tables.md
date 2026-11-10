### clownschool-batch-prefix

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| clownschool.json.gz | batch | Every(1) | 5000 | 830,416.2 | 15487.9 | 11,734.5 | 218.9 | - | - | 0.96 / 10.51 |

### clownschool-batch-sync-prefix

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| clownschool.json.gz | batch (sync) | Every(1) | 5000 | 934,356.7 | 17426.5 | 9,874.4 | 184.2 | 1,333,632.4 | 24873.3 | 1.27 / 11.91 |

### clownschool-batch-sync

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| clownschool.json.gz | batch (sync) | All | 3 | 291.9 | 1.1 | 265.1 | 1.0 | 301.4 | 1.2 | 0.50 / 0.72 |
| clownschool.json.gz | batch (sync) | Second | 5415 | 2,474,260.3 | 9678.3 | 12,561.3 | 49.1 | 3,759,663.1 | 14706.2 | 2.41 / 16.48 |
| clownschool.json.gz | batch (sync) | Pause(2) | 288 | 174,260.2 | 681.6 | 1,747.5 | 6.8 | 241,351.3 | 944.1 | 1.25 / 4.03 |
| clownschool.json.gz | batch (sync) | Pause(5) | 41 | 7,624.3 | 29.8 | 453.9 | 1.8 | 8,357.9 | 32.7 | 0.88 / 2.16 |

### clownschool-batch

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| clownschool.json.gz | batch | All | 1 | 258.4 | 1.0 | 258.4 | 1.0 | - | - | 1.22 / 1.22 |
| clownschool.json.gz | batch | Second | 4923 | 890,521.6 | 3483.3 | 12,564.6 | 49.1 | - | - | 0.92 / 9.12 |
| clownschool.json.gz | batch | Pause(2) | 180 | 16,017.2 | 62.7 | 1,224.1 | 4.8 | - | - | 0.41 / 1.36 |
| clownschool.json.gz | batch | Pause(5) | 26 | 1,007.5 | 3.9 | 334.1 | 1.3 | - | - | 0.27 / 0.59 |

### clownschool-prefix

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| clownschool.json.gz | facts | All | 1 | 2,945.0 | 54.9 | 2,945.0 | 54.9 | - | - | 266.40 / 266.40 |
| clownschool.json.gz | facts | Every(100) | 50 | 33,654.7 | 627.7 | 3,021.6 | 56.4 | - | - | 5.16 / 15.33 |
| clownschool.json.gz | facts | Every(10) | 500 | 75,795.0 | 1413.6 | 3,749.8 | 69.9 | - | - | 1.12 / 7.67 |
| clownschool.json.gz | facts | Every(1) | 5000 | 838,705.8 | 15642.5 | 11,757.5 | 219.3 | - | - | 0.88 / 9.75 |
| clownschool.json.gz | facts | Second | 1040 | 145,707.6 | 2717.6 | 4,672.4 | 87.1 | - | - | 0.77 / 4.43 |
| clownschool.json.gz | region-canonical | All | 1 | 97.7 | 1.8 | 97.7 | 1.8 | - | - | 38.23 / 38.23 |
| clownschool.json.gz | region-canonical | Every(100) | 50 | 685.7 | 12.8 | 97.7 | 1.8 | - | - | 1.14 / 4.03 |
| clownschool.json.gz | region-canonical | Every(10) | 500 | 5,076.5 | 94.7 | 97.7 | 1.8 | - | - | 0.18 / 0.54 |
| clownschool.json.gz | region-canonical | Every(1) | 5000 | 35,077.3 | 654.2 | 97.7 | 1.8 | - | - | 0.10 / 0.38 |
| clownschool.json.gz | region-canonical | Second | 1040 | 7,747.2 | 144.5 | 97.7 | 1.8 | - | - | 0.11 / 0.49 |
| clownschool.json.gz | doc | All | 1 | 7.2 | 0.1 | 7.2 | 0.1 | - | - | 0.40 / 0.40 |
| clownschool.json.gz | doc | Every(100) | 50 | 1,987.5 | 37.1 | 235.9 | 4.4 | - | - | 0.30 / 0.60 |
| clownschool.json.gz | doc | Every(10) | 500 | 63,529.3 | 1184.9 | 2,160.6 | 40.3 | - | - | 0.65 / 1.94 |
| clownschool.json.gz | doc | Every(1) | 5000 | 1,132,230.3 | 21117.0 | 22,358.8 | 417.0 | - | - | 1.22 / 4.90 |
| clownschool.json.gz | doc | Second | 1040 | 146,773.6 | 2737.4 | 4,512.4 | 84.2 | - | - | 0.66 / 2.00 |

### clownschool-sync-prefix

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| clownschool.json.gz | facts (sync) | All | 2 | 3,748.2 | 69.9 | 2,939.3 | 54.8 | 3,719.1 | 69.4 | 71.77 / 74.41 |
| clownschool.json.gz | facts (sync) | Every(100) | 95 | 94,556.0 | 1763.5 | 3,146.9 | 58.7 | 121,965.2 | 2274.7 | 5.11 / 16.83 |
| clownschool.json.gz | facts (sync) | Every(10) | 762 | 453,961.9 | 8466.8 | 4,375.1 | 81.6 | 651,172.8 | 12144.9 | 2.62 / 11.61 |
| clownschool.json.gz | facts (sync) | Every(1) | 5000 | 928,895.1 | 17324.6 | 9,888.7 | 184.4 | 1,338,923.7 | 24972.0 | 1.55 / 16.92 |
| clownschool.json.gz | facts (sync) | Second | 1155 | 429,289.4 | 8006.6 | 4,741.8 | 88.4 | 620,965.2 | 11581.5 | 2.57 / 13.98 |

### clownschool-sync

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| clownschool.json.gz | facts (sync) | All | 3 | 15,703.0 | 61.4 | 13,594.3 | 53.2 | 15,397.0 | 60.2 | 774.36 / 1485.06 |
| clownschool.json.gz | facts (sync) | Every(1000) | 49 | 164,934.8 | 645.2 | 13,701.7 | 53.6 | 283,235.5 | 1107.9 | 44.56 / 111.60 |
| clownschool.json.gz | facts (sync) | Every(100) | 442 | 903,258.1 | 3533.2 | 14,530.2 | 56.8 | 1,326,115.7 | 5187.2 | 14.45 / 41.07 |
| clownschool.json.gz | facts (sync) | Pause(2) | 288 | 579,942.0 | 2268.5 | 14,177.0 | 55.5 | 817,637.3 | 3198.3 | 17.11 / 77.80 |
| clownschool.json.gz | facts (sync) | Pause(5) | 41 | 98,395.7 | 384.9 | 13,688.0 | 53.5 | 146,238.3 | 572.0 | 49.57 / 423.54 |

### clownschool

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| clownschool.json.gz | raw | - | - | 255.7 | 1.0 | 255.7 | 1.0 | - | - | - |
| clownschool.json.gz | region-canonical | All | 1 | 457.8 | 1.8 | 457.8 | 1.8 | - | - | 234.21 / 234.21 |
| clownschool.json.gz | region-canonical | Every(1000) | 24 | 935.8 | 3.7 | 457.8 | 1.8 | - | - | 14.81 / 109.43 |
| clownschool.json.gz | region-canonical | Every(100) | 232 | 4,763.0 | 18.6 | 457.8 | 1.8 | - | - | 1.71 / 22.46 |
| clownschool.json.gz | region-canonical | Every(10) | 2314 | 37,597.9 | 147.1 | 457.8 | 1.8 | - | - | 0.32 / 2.65 |
| clownschool.json.gz | region-canonical | Every(1) | 23136 | 267,065.3 | 1044.6 | 457.8 | 1.8 | - | - | 0.14 / 0.72 |
| clownschool.json.gz | region-canonical | Second | 4923 | 61,277.1 | 239.7 | 457.8 | 1.8 | - | - | 0.22 / 2.04 |
| clownschool.json.gz | region-canonical | Pause(2) | 180 | 3,157.6 | 12.4 | 457.8 | 1.8 | - | - | 1.99 / 22.37 |
| clownschool.json.gz | region-canonical | Pause(5) | 26 | 776.4 | 3.0 | 457.8 | 1.8 | - | - | 9.07 / 138.43 |
| clownschool.json.gz | region-buffered | All | 1 | 458.3 | 1.8 | 457.8 | 1.8 | - | - | 337.06 / 337.06 |
| clownschool.json.gz | region-buffered | Every(1000) | 24 | 1,005.3 | 3.9 | 457.8 | 1.8 | - | - | 11.18 / 40.43 |
| clownschool.json.gz | region-buffered | Every(100) | 232 | 3,196.7 | 12.5 | 457.8 | 1.8 | - | - | 1.77 / 22.29 |
| clownschool.json.gz | region-buffered | Every(10) | 2314 | 18,789.5 | 73.5 | 457.8 | 1.8 | - | - | 0.20 / 3.52 |
| clownschool.json.gz | region-buffered | Every(1) | 23136 | 174,775.1 | 683.6 | 457.8 | 1.8 | - | - | 0.05 / 0.09 |
| clownschool.json.gz | region-buffered | Second | 4923 | 38,390.1 | 150.2 | 457.8 | 1.8 | - | - | 0.12 / 1.75 |
| clownschool.json.gz | region-buffered | Pause(2) | 180 | 2,548.9 | 10.0 | 457.8 | 1.8 | - | - | 2.44 / 26.94 |
| clownschool.json.gz | region-buffered | Pause(5) | 26 | 782.1 | 3.1 | 457.8 | 1.8 | - | - | 11.34 / 187.30 |
| clownschool.json.gz | doc | All | 1 | 23.8 | 0.1 | 23.8 | 0.1 | - | - | 0.45 / 0.45 |
| clownschool.json.gz | doc | Every(1000) | 24 | 788.5 | 3.1 | 317.9 | 1.2 | - | - | 0.34 / 0.41 |
| clownschool.json.gz | doc | Every(100) | 232 | 13,559.3 | 53.0 | 2,909.4 | 11.4 | - | - | 0.45 / 1.16 |
| clownschool.json.gz | doc | Every(10) | 2314 | 334,510.9 | 1308.5 | 29,262.2 | 114.5 | - | - | 0.82 / 4.63 |
| clownschool.json.gz | doc | Second | 4923 | 880,007.8 | 3442.2 | 61,914.3 | 242.2 | - | - | 1.02 / 7.09 |
| clownschool.json.gz | doc | Pause(2) | 180 | 9,709.6 | 38.0 | 2,221.9 | 8.7 | - | - | 0.32 / 0.76 |
| clownschool.json.gz | doc | Pause(5) | 26 | 833.3 | 3.3 | 321.4 | 1.3 | - | - | 0.26 / 0.39 |
| clownschool.json.gz | facts | All | 1 | 13,588.8 | 53.2 | 13,588.8 | 53.2 | - | - | 5810.92 / 5810.92 |
| clownschool.json.gz | facts | Every(1000) | 24 | 116,250.4 | 454.7 | 13,644.9 | 53.4 | - | - | 86.03 / 343.24 |
| clownschool.json.gz | facts | Every(100) | 232 | 458,110.1 | 1791.9 | 13,978.5 | 54.7 | - | - | 15.86 / 40.93 |
| clownschool.json.gz | facts | Every(10) | 2314 | 773,757.9 | 3026.6 | 17,565.4 | 68.7 | - | - | 2.49 / 24.81 |
| clownschool.json.gz | facts | Pause(2) | 180 | 315,641.4 | 1234.7 | 13,892.7 | 54.3 | - | - | 17.03 / 86.49 |
| clownschool.json.gz | facts | Pause(5) | 26 | 60,779.6 | 237.7 | 13,652.6 | 53.4 | - | - | 93.77 / 1010.15 |

### friendsforever-batch-prefix

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| friendsforever.json.gz | batch | Every(1) | 5000 | 717,008.0 | 13461.1 | 11,731.1 | 220.2 | - | - | 0.84 / 12.04 |

### friendsforever-batch-sync-prefix

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| friendsforever.json.gz | batch (sync) | Every(1) | 5000 | 852,032.3 | 15996.1 | 9,764.6 | 183.3 | 1,214,271.3 | 22796.8 | 1.29 / 13.70 |

### friendsforever-batch-sync

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| friendsforever.json.gz | batch (sync) | All | 2 | 290.1 | 1.1 | 280.0 | 1.0 | 291.5 | 1.1 | 0.55 / 0.56 |
| friendsforever.json.gz | batch (sync) | Every(1000) | 53 | 10,700.2 | 39.1 | 482.3 | 1.8 | 10,967.0 | 40.0 | 0.91 / 1.29 |
| friendsforever.json.gz | batch (sync) | Every(100) | 496 | 356,293.5 | 1300.9 | 2,654.7 | 9.7 | 526,207.0 | 1921.3 | 1.53 / 5.56 |

### friendsforever-batch

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| friendsforever.json.gz | batch | All | 1 | 276.6 | 1.0 | 276.6 | 1.0 | - | - | 0.97 / 0.97 |
| friendsforever.json.gz | batch | Every(1000) | 27 | 1,102.0 | 4.0 | 341.3 | 1.2 | - | - | 0.25 / 0.39 |
| friendsforever.json.gz | batch | Every(100) | 261 | 30,288.4 | 110.6 | 1,900.4 | 6.9 | - | - | 0.52 / 1.94 |

### friendsforever-prefix

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| friendsforever.json.gz | facts | All | 1 | 2,938.0 | 55.2 | 2,938.0 | 55.2 | - | - | 274.36 / 274.36 |
| friendsforever.json.gz | facts | Every(100) | 50 | 36,533.3 | 685.9 | 3,011.2 | 56.5 | - | - | 9.20 / 22.90 |
| friendsforever.json.gz | facts | Every(10) | 500 | 89,806.2 | 1686.0 | 3,743.3 | 70.3 | - | - | 1.81 / 15.77 |
| friendsforever.json.gz | facts | Every(1) | 5000 | 845,191.5 | 15867.7 | 11,747.7 | 220.6 | - | - | 0.95 / 10.82 |
| friendsforever.json.gz | region-canonical | All | 1 | 96.4 | 1.8 | 96.4 | 1.8 | - | - | 39.21 / 39.21 |
| friendsforever.json.gz | region-canonical | Every(100) | 50 | 852.5 | 16.0 | 96.4 | 1.8 | - | - | 0.96 / 4.81 |
| friendsforever.json.gz | region-canonical | Every(10) | 500 | 6,188.6 | 116.2 | 96.4 | 1.8 | - | - | 0.25 / 1.39 |
| friendsforever.json.gz | region-canonical | Every(1) | 5000 | 40,931.7 | 768.5 | 96.4 | 1.8 | - | - | 0.12 / 0.68 |
| friendsforever.json.gz | doc | All | 1 | 7.2 | 0.1 | 7.2 | 0.1 | - | - | 0.33 / 0.33 |
| friendsforever.json.gz | doc | Every(100) | 50 | 2,035.3 | 38.2 | 238.4 | 4.5 | - | - | 0.51 / 0.83 |
| friendsforever.json.gz | doc | Every(10) | 500 | 70,294.6 | 1319.7 | 2,167.3 | 40.7 | - | - | 0.83 / 3.16 |
| friendsforever.json.gz | doc | Every(1) | 5000 | 849,089.6 | 15940.9 | 22,401.5 | 420.6 | - | - | 1.04 / 6.38 |

### friendsforever-sync-prefix

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| friendsforever.json.gz | facts (sync) | All | 2 | 3,872.8 | 72.7 | 2,939.0 | 55.2 | 3,736.7 | 70.2 | 73.19 / 75.92 |
| friendsforever.json.gz | facts (sync) | Every(100) | 100 | 91,606.5 | 1719.8 | 3,143.0 | 59.0 | 134,175.4 | 2519.0 | 3.31 / 9.43 |
| friendsforever.json.gz | facts (sync) | Every(10) | 772 | 506,556.9 | 9510.1 | 4,389.0 | 82.4 | 716,964.9 | 13460.3 | 3.08 / 14.80 |
| friendsforever.json.gz | facts (sync) | Every(1) | 5000 | 883,672.7 | 16590.1 | 9,771.8 | 183.5 | 1,266,034.7 | 23768.6 | 1.46 / 13.97 |

### friendsforever-sync

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| friendsforever.json.gz | facts (sync) | All | 2 | 16,291.6 | 59.5 | 15,269.1 | 55.8 | 16,196.7 | 59.1 | 1728.43 / 1964.56 |
| friendsforever.json.gz | facts (sync) | Every(1000) | 53 | 207,849.2 | 758.9 | 15,378.7 | 56.2 | 331,818.3 | 1211.5 | 69.19 / 170.79 |
| friendsforever.json.gz | facts (sync) | Every(100) | 496 | 1,158,269.1 | 4229.1 | 16,320.9 | 59.6 | 1,692,828.5 | 6180.9 | 17.12 / 61.43 |

### friendsforever

| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| friendsforever.json.gz | raw | - | - | 273.9 | 1.0 | 273.9 | 1.0 | - | - | - |
| friendsforever.json.gz | region-canonical | All | 1 | 502.9 | 1.8 | 502.9 | 1.8 | - | - | 297.93 / 297.93 |
| friendsforever.json.gz | region-canonical | Every(1000) | 27 | 1,010.8 | 3.7 | 502.9 | 1.8 | - | - | 14.20 / 108.72 |
| friendsforever.json.gz | region-canonical | Every(100) | 261 | 5,493.9 | 20.1 | 502.9 | 1.8 | - | - | 2.19 / 21.76 |
| friendsforever.json.gz | region-canonical | Every(10) | 2608 | 40,060.7 | 146.3 | 502.9 | 1.8 | - | - | 0.38 / 4.56 |
| friendsforever.json.gz | region-canonical | Every(1) | 26078 | 313,640.5 | 1145.2 | 502.9 | 1.8 | - | - | 0.14 / 0.78 |
| friendsforever.json.gz | region-buffered | All | 1 | 503.2 | 1.8 | 502.9 | 1.8 | - | - | 382.91 / 382.91 |
| friendsforever.json.gz | region-buffered | Every(1000) | 27 | 1,098.5 | 4.0 | 502.9 | 1.8 | - | - | 14.86 / 63.91 |
| friendsforever.json.gz | region-buffered | Every(100) | 261 | 3,594.9 | 13.1 | 502.9 | 1.8 | - | - | 1.86 / 31.07 |
| friendsforever.json.gz | region-buffered | Every(10) | 2608 | 21,853.3 | 79.8 | 502.9 | 1.8 | - | - | 0.21 / 3.71 |
| friendsforever.json.gz | region-buffered | Every(1) | 26078 | 204,276.3 | 745.9 | 502.9 | 1.8 | - | - | 0.05 / 0.10 |
| friendsforever.json.gz | doc | All | 1 | 24.0 | 0.1 | 24.0 | 0.1 | - | - | 0.54 / 0.54 |
| friendsforever.json.gz | doc | Every(1000) | 27 | 954.2 | 3.5 | 371.8 | 1.4 | - | - | 0.41 / 0.55 |
| friendsforever.json.gz | doc | Every(100) | 261 | 16,127.8 | 58.9 | 3,422.3 | 12.5 | - | - | 0.56 / 1.36 |
| friendsforever.json.gz | doc | Every(10) | 2608 | 407,442.7 | 1487.7 | 34,429.7 | 125.7 | - | - | 0.97 / 6.62 |
| friendsforever.json.gz | doc | Every(1) | 26078 | 5,320,925.6 | 19427.9 | 342,421.5 | 1250.3 | - | - | 1.40 / 21.06 |
| friendsforever.json.gz | facts | All | 1 | 15,267.4 | 55.7 | 15,267.4 | 55.7 | - | - | 7925.25 / 7925.25 |
| friendsforever.json.gz | facts | Every(1000) | 27 | 156,965.2 | 573.1 | 15,321.8 | 55.9 | - | - | 106.16 / 270.64 |
| friendsforever.json.gz | facts | Every(100) | 261 | 632,975.9 | 2311.1 | 15,700.4 | 57.3 | - | - | 22.76 / 58.36 |
| friendsforever.json.gz | facts | Every(10) | 2608 | 1,182,107.6 | 4316.1 | 19,757.6 | 72.1 | - | - | 3.41 / 18.85 |

