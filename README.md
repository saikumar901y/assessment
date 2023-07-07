# Apache Beam Join and Data Transformation

This project demonstrates the use of Apache Beam to perform a join operation on two input datasets (`dataset1` and `dataset2`) and calculate certain values based on specific conditions.

## Requirements

- Python 3.x
- Apache Beam

## Usage

1. Install the required dependencies by running the following command:

2. Update the `dataset1_path`, `dataset2_path`, and `output_path` variables in the `main.py` file with the paths to your input datasets and desired output file.

3. Execute the Apache Beam pipeline by running the following command:


4. The resulting data will be written to the specified output file.

## Code Explanation

The code follows these main steps:

1. Import the necessary libraries, including `pandas` for data manipulation and `apache_beam` for Apache Beam.

2. Define the `calculate_values` function to perform the desired calculations and transformations on the merged data.

3. Create the `JoinTransform` class, which is a custom Apache Beam `DoFn` that applies the `calculate_values` function to the input datasets.

4. Define the `run_pipeline` function to execute the Apache Beam pipeline. It sets up the pipeline, reads the input datasets, applies the join and calculation logic using the `JoinTransform` class, and writes the resulting data to the output file.

5. In the `__main__` block, specify the paths to the input datasets and output file, and call the `run_pipeline` function to execute the pipeline.

## License

This project is licensed under the [MIT License](LICENSE).
