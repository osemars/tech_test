# Dependencies
* Pandas
* MatPlotLib.

# Data Processing
* find_min_errs() and find_max_errs()were used to find all elements withing the columns > 10000 or < -10000.
  999999.0 from Active Power column was the only element > 10000 and -295609.0 from Current < -10000.
* They were replaced by the mean of the upper and lower elements within thier columns. 
* New values were 0.65 Watt and 0.025 amp.
* 1 duplicate rows was dropped.
* No na values were found.

# Issues and Conclusion
* Laptop might crash running the code for interpolation and resampling.
* I'll keep working on it, to fix it.
