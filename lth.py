import numpy as np
from scipy import stats

# Sample data
data = np.array([1, 2, 2, 3, 4, 4, 4, 5])

# Calculate mode
mode_result = stats.mode(data)

print("Mode:", mode_result.mode)
print("Frequency:", mode_result.count)
