
public class Main {
	public static void main (String[] args) {
		System.out.println("start");
		System.loadLibrary("test");
		Client c = new Client("127.0.0.1", 1981, "127.0.0.1", 1982);
        int[] status = new int[1];
        status[0] = 0;
        //long fd = c.open("/file1", O_CREAT | O_RDWR, 0777, 3, 4096, status);
        long fd = c.open("/file1", 0x0102, 0777, 3, 4096, status);
        System.out.println("open fd " + fd + " status " + status[0]);

        byte[] data = "hi".getBytes();
        long[] data_sz = { 2L };
        long reqid = c.write(fd, data, data_sz, 3, status);
        reqid = c.loop(reqid, -1, status);

        c.close(fd, status);
        System.out.println("close fd " + fd + " status " + status[0]);
		
		System.out.println("finish");
	}
}
