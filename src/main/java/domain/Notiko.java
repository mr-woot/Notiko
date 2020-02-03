package domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * Project: kstreamsdemo
 * Contributed By: Tushar Mudgal
 * On: 29/01/20 | 6:05 PM
 */
public class Notiko implements Serializable {
    private static final long serialVersionUID = 1L;
    private String siteId;
    private String payLoad;
    private String eventName;
    private String mobileNumber;
    private long nextCommunicationTimestamp;
    private int nextCommunicationNumber;
    private int totalCommunicationNumber;
    private String messageID;

    public static Notiko createNotikoJsonNode(JsonNode obj) throws JsonProcessingException {
        String messageID = obj.get("MessageID").textValue();
        JsonNode payload = obj.get("Payload");
        Notiko notiko = new ObjectMapper().treeToValue(payload, Notiko.class);
        notiko.setMessageID(messageID);
        return notiko;
    }

    public Notiko() {
    }

    public Notiko(String siteId,
                  String payLoad,
                  String eventName,
                  String mobileNumber,
                  long nextCommunicationTimestamp,
                  int nextCommunicationNumber,
                  int totalCommunicationNumber,
                  String messageID) {
        this.siteId = siteId;
        this.payLoad = payLoad;
        this.eventName = eventName;
        this.mobileNumber = mobileNumber;
        this.nextCommunicationTimestamp = nextCommunicationTimestamp;
        this.nextCommunicationNumber = nextCommunicationNumber;
        this.totalCommunicationNumber = totalCommunicationNumber;
        this.messageID = messageID;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getPayLoad() {
        return payLoad;
    }

    public void setPayLoad(String payLoad) {
        this.payLoad = payLoad;
    }

    public String geteventName() {
        return eventName;
    }

    public void seteventName(String eventName) {
        this.eventName = eventName;
    }

    public String getMobileNumber() {
        return mobileNumber;
    }

    public void setMobileNumber(String mobileNumber) {
        this.mobileNumber = mobileNumber;
    }

    public long getNextCommunicationTimestamp() {
        return nextCommunicationTimestamp;
    }

    public void setNextCommunicationTimestamp(long nextCommunicationTimestamp) {
        this.nextCommunicationTimestamp = nextCommunicationTimestamp;
    }

    public int getNextCommunicationNumber() {
        return nextCommunicationNumber;
    }

    public void setNextCommunicationNumber(int nextCommunicationNumber) {
        this.nextCommunicationNumber = nextCommunicationNumber;
    }

    public int getTotalCommunicationNumber() {
        return totalCommunicationNumber;
    }

    public void setTotalCommunicationNumber(int totalCommunicationNumber) {
        this.totalCommunicationNumber = totalCommunicationNumber;
    }

    public String getMessageID() {
        return messageID;
    }

    public void setMessageID(String messageID) {
        this.messageID = messageID;
    }

    @Override
    public String toString() {
        return "Notiko{" +
                "siteId='" + siteId + '\'' +
                ", payLoad='" + payLoad + '\'' +
                ", eventName='" + eventName + '\'' +
                ", mobileNumber='" + mobileNumber + '\'' +
                ", nextCommunicationTimestamp=" + nextCommunicationTimestamp +
                ", nextCommunicationNumber=" + nextCommunicationNumber +
                ", totalCommunicationNumber=" + totalCommunicationNumber +
                ", messageID='" + messageID + '\'' +
                '}';
    }
}

