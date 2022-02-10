from celery import Celery
app = Celery('tasks1', backend='redis://localhost:6579', broken='pyamqp://guest@localhost:5672')

@app.task(acks_late=True)
def count(files):
	wc = {}
	for file in files:
		with open(file, mode='r', newline='\r') as fh:
			for text in fh:
				if text == '\n':
					continue
				sp = text.split(',')[4:-2]
				tweet = " ".join(sp)
				for word in tweet.split(" "):
					if word not in wc:
						wc[word] = 0
					wc[word] = wc[word] + 1
	return wc
