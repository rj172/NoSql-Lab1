package ques4;

public class Summary {
    int requests;
    int downloadSize;

    Summary() {
    }

    Summary(int requests, int downloadSize) {
        this.requests = requests;
        this.downloadSize = downloadSize;
    }

    public int getDownloadSize() {
        return downloadSize;
    }

    public void setDownloadSize(int downloadSize) {
        this.downloadSize = downloadSize;
    }

    public int getRequests() {
        return requests;
    }

    public void setRequests(int requests) {
        this.requests = requests;
    }
}