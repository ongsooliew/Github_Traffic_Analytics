**GitHub Commit Traffic Analytics**

 1. Make sure you have Anaconda 3.8 and Postgres installed on your local machine
 2. Git Clone the repo to your machine
 3. Go into the main folder "GitHub Traffic Analytics"
 4. Run "bash setup.sh" to do a one-off setup. 
 5. Open config.yaml using any texteditor and fill in the followings:
			a) GitHub Token (Required)
			b) Postgres Credentials (Required)
			c) Earliest Date of Interest. (Optional)
 6. Kick start the batch job with  "bash run_job.sh"
 7. Outputs all for 3 questions will be saved in the Results folder



Do Note that Github has changed the way it captures the committer's fields. A lot of records ended up with "Github" as the default committer name. To make the assignment meaningful, I have elected to substitute those values with the values from "authorname" field instead.  

For more details, refer to https://github.community/t/authorship-of-merge-commits-made-by-github-apps-changed/2971/21
