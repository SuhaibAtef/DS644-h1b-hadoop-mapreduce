import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * H1BMultiQuestionAnalysis
 *
 * A single MapReduce job that computes aggregates to answer:
 *   1) How do wages differ between tech vs non-tech occupations?
 *   2) For which occupations do H-1B workers earn the most in 2024?
 *   3) Which states rely most heavily on H-1B workers in 2024?
 *   4) How often is the worksite in a different state than the employer?
 *
 * Input: 2024 H-1B LCA CSV in DOL layout (96 columns).
 *
 * For each H-1B, CERTIFIED, full-time (FULL_TIME_POSITION == 'Y') record:
 *   - compute annual wage from WAGE_RATE_OF_PAY_* and WAGE_UNIT_OF_PAY
 *   - use TOTAL_WORKER_POSITIONS as worker count (default 1)
 *
 * Mapper emits four key types:
 *   CAT|TECH or CAT|NON_TECH
 *   SOC|<SOC_CODE>|<SOC_TITLE>
 *   STATE|<WORKSITE_STATE>
 *   FLOW|<EMPLOYER_STATE>-><WORKSITE_STATE>
 *
 * Reducer aggregates for each key:
 *   totalWorkers = sum(workers)
 *   avgAnnualWage = sum(annualWage * workers) / totalWorkers
 *
 * Output line format:
 *   KEY \t TOTAL_WORKERS \t AVG_ANNUAL_WAGE
 */
public class H1BMultiQuestionAnalysis extends Configured implements Tool {

    // Column indices (0-based) from LCA_Record_Layout_FY2023 (96 columns).
    // These match the Kaggle H1B LCA Disclosure Data 2020–2024 layout.
    private static final int CASE_STATUS_INDEX            = 1;   // CASE_STATUS
    private static final int VISA_CLASS_INDEX             = 5;   // VISA_CLASS
    private static final int SOC_CODE_INDEX               = 7;   // SOC_CODE
    private static final int SOC_TITLE_INDEX              = 8;   // SOC_TITLE
    private static final int FULL_TIME_POSITION_INDEX     = 9;   // FULL_TIME_POSITION
    private static final int TOTAL_WORKER_POSITIONS_INDEX = 12;  // TOTAL_WORKER_POSITIONS
    private static final int EMPLOYER_STATE_INDEX         = 24;  // EMPLOYER_STATE
    private static final int WORKSITE_STATE_INDEX         = 69;  // WORKSITE_STATE
    private static final int WAGE_RATE_OF_PAY_FROM_INDEX  = 71;  // WAGE_RATE_OF_PAY_FROM
    private static final int WAGE_RATE_OF_PAY_TO_INDEX    = 72;  // WAGE_RATE_OF_PAY_TO
    private static final int WAGE_UNIT_OF_PAY_INDEX       = 73;  // WAGE_UNIT_OF_PAY

    // --------- CSV helpers ---------

    // Regex to split CSV on commas that are NOT inside quotes.
    private static final String CSV_SPLIT_REGEX =
            ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    private static String stripQuotes(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        return s.trim();
    }

    // --------- Wage conversion ---------

    /**
     * Convert a base wage and unit-of-pay into annual wage.
     * Valid units (case-insensitive): Hour, Week, Bi-Weekly, Month, Year.
     * Returns -1.0 for unknown/unsupported units.
     */
    private static double convertToAnnual(double wage, String unitRaw) {
        if (unitRaw == null) return -1.0;
        String unit = unitRaw.trim().toLowerCase();

        if (unit.equals("hour")) {
            return wage * 2080.0;        // 40h * 52 weeks
        } else if (unit.equals("week")) {
            return wage * 52.0;
        } else if (unit.equals("bi-weekly") || unit.equals("biweekly")) {
            return wage * 26.0;
        } else if (unit.equals("month")) {
            return wage * 12.0;
        } else if (unit.equals("year")) {
            return wage;
        } else {
            return -1.0;                 // unknown unit
        }
    }

    // --------- Tech vs non-tech classification ---------

    /**
     * Very simple tech classification:
     *   TECH if SOC_CODE starts with "15-" (Computer & Mathematical Occupations).
     *   otherwise NON-TECH.
     */
    private static boolean isTechOccupation(String socCode) {
        if (socCode == null) return false;
        socCode = socCode.trim();
        return socCode.startsWith("15-");
    }

    // --------- Custom Writable to hold aggregates ---------

    public static class AggregateWritable implements Writable {
        private DoubleWritable wageTimesWorkers;  // sum(annualWage * workers)
        private LongWritable workerCount;         // sum(workers)

        public AggregateWritable() {
            this.wageTimesWorkers = new DoubleWritable(0.0);
            this.workerCount = new LongWritable(0L);
        }

        public AggregateWritable(double wageTimesWorkers, long workerCount) {
            this.wageTimesWorkers = new DoubleWritable(wageTimesWorkers);
            this.workerCount = new LongWritable(workerCount);
        }

        public void set(double wageTimesWorkers, long workerCount) {
            this.wageTimesWorkers.set(wageTimesWorkers);
            this.workerCount.set(workerCount);
        }

        public double getWageTimesWorkers() {
            return wageTimesWorkers.get();
        }

        public long getWorkerCount() {
            return workerCount.get();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            wageTimesWorkers.write(out);
            workerCount.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            wageTimesWorkers.readFields(in);
            workerCount.readFields(in);
        }

        @Override
        public String toString() {
            return wageTimesWorkers + "\t" + workerCount;
        }
    }

    // --------- Mapper ---------

    public static class H1BMapper
            extends Mapper<LongWritable, Text, Text, AggregateWritable> {

        private final Text outKey = new Text();
        private final AggregateWritable outVal = new AggregateWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line == null || line.isEmpty()) {
                return;
            }

            String[] fields = line.split(CSV_SPLIT_REGEX, -1);
            if (fields.length < 74) { // need at least up to WAGE_UNIT_OF_PAY
                return;
            }

            // Skip header row(s)
            String firstCol = stripQuotes(fields[0]);
            if ("CASE_NUMBER".equalsIgnoreCase(firstCol)) {
                return;
            }

            // Filter on CASE_STATUS
            String caseStatus = stripQuotes(fields[CASE_STATUS_INDEX]);
            if (caseStatus == null) return;
            String statusUpper = caseStatus.toUpperCase();
            // Use approved cases only: CERTIFIED & CERTIFIED-WITHDRAWN
            if (!statusUpper.equals("CERTIFIED") &&
                !statusUpper.equals("CERTIFIED-WITHDRAWN")) {
                return;
            }

            // Exclude E-3 and H-1B1 cases present in the LCA disclosure file.
            String visaClass = stripQuotes(fields[VISA_CLASS_INDEX]);
            if (visaClass == null || !visaClass.equalsIgnoreCase("H-1B")) {
                return;
            }

            // Filter to full-time positions
            String fullTime = stripQuotes(fields[FULL_TIME_POSITION_INDEX]);
            if (fullTime == null ||
                !fullTime.trim().equalsIgnoreCase("Y")) {
                return;
            }

            // Extract core fields
            String socCode = stripQuotes(fields[SOC_CODE_INDEX]);
            String socTitle = stripQuotes(fields[SOC_TITLE_INDEX]);
            String employerState = stripQuotes(fields[EMPLOYER_STATE_INDEX]);
            String worksiteState = stripQuotes(fields[WORKSITE_STATE_INDEX]);

            String wageFromStr = stripQuotes(fields[WAGE_RATE_OF_PAY_FROM_INDEX]);
            String wageToStr   = stripQuotes(fields[WAGE_RATE_OF_PAY_TO_INDEX]);
            String wageUnit    = stripQuotes(fields[WAGE_UNIT_OF_PAY_INDEX]);
            String workersStr  = stripQuotes(fields[TOTAL_WORKER_POSITIONS_INDEX]);

            if (wageFromStr == null || wageFromStr.isEmpty()) {
                return;
            }

            double wageFrom;
            double wageTo;
            double baseWage;
            long workers;

            try {
                wageFrom = Double.parseDouble(wageFromStr);

                if (wageToStr != null && !wageToStr.isEmpty()) {
                    wageTo = Double.parseDouble(wageToStr);
                    baseWage = (wageFrom + wageTo) / 2.0;
                } else {
                    baseWage = wageFrom;
                }

                if (workersStr == null || workersStr.isEmpty()) {
                    workers = 1L;
                } else {
                    workers = Long.parseLong(workersStr);
                    if (workers <= 0L) workers = 1L;
                }
            } catch (NumberFormatException e) {
                // bad numeric fields -> skip
                return;
            }

            double annualWage = convertToAnnual(baseWage, wageUnit);
            if (annualWage <= 0.0) {
                return;
            }

            double wageTimesWorkers = annualWage * (double) workers;

            // 1) Tech vs Non-Tech category
            if (socCode != null && !socCode.isEmpty()) {
                String cat = isTechOccupation(socCode) ? "TECH" : "NON_TECH";
                outKey.set("CAT|" + cat);
                outVal.set(wageTimesWorkers, workers);
                context.write(outKey, outVal);
            }

            // 2) Occupation-level stats
            if (socCode != null && !socCode.isEmpty() &&
                socTitle != null && !socTitle.isEmpty()) {
                outKey.set("SOC|" + socCode + "|" + socTitle);
                outVal.set(wageTimesWorkers, workers);
                context.write(outKey, outVal);
            }

            // 3) Worksite state stats
            if (worksiteState != null && !worksiteState.isEmpty()) {
                outKey.set("STATE|" + worksiteState);
                outVal.set(wageTimesWorkers, workers);
                context.write(outKey, outVal);
            }

            // 4) Employer vs Worksite state flows
            if (employerState != null && !employerState.isEmpty() &&
                worksiteState != null && !worksiteState.isEmpty()) {

                outKey.set("FLOW|" + employerState + "->" + worksiteState);
                outVal.set(wageTimesWorkers, workers);
                context.write(outKey, outVal);
            }
        }
    }

    // --------- Reducer ---------

    public static class H1BReducer
            extends Reducer<Text, AggregateWritable, Text, Text> {

        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key,
                              Iterable<AggregateWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            double sumWageTimesWorkers = 0.0;
            long sumWorkers = 0L;

            for (AggregateWritable val : values) {
                sumWageTimesWorkers += val.getWageTimesWorkers();
                sumWorkers += val.getWorkerCount();
            }

            if (sumWorkers <= 0L) {
                return;
            }

            double avgAnnualWage = sumWageTimesWorkers / (double) sumWorkers;
            String result = String.format("%d\t%.2f", sumWorkers, avgAnnualWage);

            outValue.set(result);
            context.write(key, outValue);
        }
    }

    // --------- Driver ---------

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: H1BMultiQuestionAnalysis <input> <output>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "H1B Multi-Question Analysis 2024");

        job.setJarByClass(H1BMultiQuestionAnalysis.class);

        job.setMapperClass(H1BMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AggregateWritable.class);

        job.setReducerClass(H1BReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
                new Configuration(),
                new H1BMultiQuestionAnalysis(),
                args
        );
        System.exit(exitCode);
    }
}
