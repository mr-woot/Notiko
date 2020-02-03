import service.NotikoService;

/**
 * Project: kstreamsdemo
 * Contributed By: Tushar Mudgal
 * On: 29/01/20 | 10:02 AM
 */
public class StreamsApplication {
    public static void main(String[] args) {
        // ## call to run notiko streams service
        // this service sends inserts or updated inserts based on key (mobileNumber+siteId)
        NotikoService service = new NotikoService();
        service.run();
    }
}
