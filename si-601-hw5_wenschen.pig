
-- Step 1

R = LOAD '/user/cteplovs/yelp_academic_dataset_review.json' USING JsonLoader('votes:map[], user_id:chararray, review_id:chararray,stars:int, date:chararray, text:chararray, type:chararray, business_id:chararray');
all_review = FOREACH R GENERATE stars, FLATTEN(TOKENIZE(text));
all_review = FOREACH all_review GENERATE $0 AS stars, LOWER($1) AS word;

-- Step 2

stream1 = GROUP all_review BY word;
stream1 = FOREACH stream1 GENERATE group AS word, COUNT(all_review) AS all_word_count;
STORE stream1 INTO 'output-step-2a';

positive_review = FILTER all_review BY stars >= 5;
positive_review = GROUP positive_review BY word;
stream2 = FOREACH positive_review GENERATE group AS word, COUNT(positive_review) AS positive_word_count;
STORE stream2 INTO 'output-step-2b';
 
negative_review = FILTER all_review BY stars <= 2;
negative_review = GROUP negative_review BY word;
stream3 = FOREACH negative_review GENERATE group AS word, COUNT(negative_review) AS negative_word_count;
STORE stream3 INTO 'output-step-2c';

-- Step 3

stream1 = FILTER stream1 BY all_word_count > 1000;

positive_joint = JOIN stream1 BY word, stream2 BY word;
STORE positive_joint INTO 'output-step-3a';

negative_joint = JOIN stream1 BY word, stream3 BY word;
STORE negative_joint INTO 'output-step-3b';

-- Step 4

all_words = GROUP stream1 ALL;
total_word_count = FOREACH all_words GENERATE SUM(stream1.all_word_count) AS total;

all_positive_words = GROUP stream2 ALL;
total_positive_count = FOREACH all_positive_words GENERATE SUM(stream2.positive_word_count) AS total;

all_negative_words = GROUP stream3 ALL;
total_negative_count = FOREACH all_negative_words GENERATE SUM(stream3.negative_word_count) AS total;

output_positive = FOREACH positive_joint GENERATE stream1::word AS word, LOG(stream2::positive_word_count) - LOG(total_positive_count.total) - LOG(stream1::all_word_count) + LOG(total_word_count.total) AS Positivity;
output_positive = ORDER output_positive BY Positivity DESC;

output_negative = FOREACH negative_joint GENERATE stream1::word AS word, LOG(stream3::negative_word_count) - LOG(total_negative_count.total) - LOG(stream1::all_word_count) + LOG(total_word_count.total) AS Negativity;
output_negative = ORDER output_negative BY Negativity DESC;

STORE output_positive INTO 'output-positive';
STORE output_negative INTO 'output-negative';
