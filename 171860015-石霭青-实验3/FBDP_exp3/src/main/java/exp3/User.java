package exp3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class User implements WritableComparable<User>{

    private int user_id;        //买家id
    private int item_id;        //商品id
    private int cat_id;         //商品类别id
    private int merchant_id;    //卖家id
    private int brand_id;       //品牌id
    private int month;          //交易时间：月
    private int day;            //交易时间：日
    private int action;         //行为,取值范围{0,1,2,3},0表示点击，1表示加入购物车，2表示购买，3表示关注商品
    private int age_range;      //买家年龄分段：1表示年龄<18,2表示年龄在[18,24]，3表示年龄在[25,29]，4表示年龄在[30,34]，
                                // 5表示年龄在[35,39]，6表示年龄在[40,49]，7和8表示年龄>=50，0和NULL则表示未知
    private int gender;         //性别:0表示女性，1表示男性，2和NULL表示未知
    private String province;    //收货地址省份

    public User() {}

    public User(int User_id, int Item_id, int Cat_id, int Merchant_id, int Brand_id,
                int Month, int Day, int Action, int Age_range, int Gender, String Province) {
        this.user_id = User_id;
        this.item_id = Item_id;
        this.cat_id = Cat_id;
        this.merchant_id = Merchant_id;
        this.brand_id = Brand_id;
        this.month = Month;
        this.day = Day;
        this.action = Action;
        this.age_range = Age_range;
        this.gender = Gender;
        this.province = Province;
    }

    public User(String line) {
        String[] value = line.split(",");
        this.user_id = value[0].equals("") ? -1:Integer.parseInt(value[0]);
        this.item_id = value[1].equals("") ? -1:Integer.parseInt(value[1]);
        this.cat_id = value[2].equals("") ? -1:Integer.parseInt(value[2]);
        this.merchant_id = value[3].equals("") ? -1:Integer.parseInt(value[3]);
        this.brand_id = value[4].equals("") ? -1:Integer.parseInt(value[4]);
        this.month = value[5].equals("") ? -1:Integer.parseInt(value[5]);
        this.day = value[6].equals("") ? -1:Integer.parseInt(value[6]);
        this.action = value[7].equals("") ? -1:Integer.parseInt(value[7]);
        this.age_range = value[8].equals("") ? -1:Integer.parseInt(value[8]);
        this.gender = value[9].equals("") ? -1:Integer.parseInt(value[9]);
        this.province = value[10];
//        this.user_id = Integer.parseInt(value[0]);
//        this.item_id = Integer.parseInt(value[1]);
//        this.cat_id = Integer.parseInt(value[2]);
//        this.merchant_id = Integer.parseInt(value[3]);
//        this.brand_id = Integer.parseInt(value[4]);
//        this.month = Integer.parseInt(value[5]);
//        this.day = Integer.parseInt(value[6]);
//        this.action = Integer.parseInt(value[7]);
//        this.age_range = Integer.parseInt(value[8]);
//        this.gender = Integer.parseInt(value[9]);
//        this.province = value[10];
    }

    public int getItem_id() {
        return this.item_id;
    }

    public int getAction() {
        return this.action;
    }

    public void setItem_id(int key) {
        this.item_id = key;
    }

    public void setAction(int value) {
        this.action = value;
    }

    @Override
    public String toString() {
        return this.user_id + "," + this.item_id + "," + this.cat_id + "," + this.merchant_id + ","
                + this.brand_id + "," + this.month + "," + this.day + "," + this.action + ","
                + this.age_range + "," + this.gender + "," + this.province;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.user_id = in.readInt();
        this.item_id = in.readInt();
        this.cat_id = in.readInt();
        this.merchant_id = in.readInt();
        this.brand_id = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.action = in.readInt();
        this.age_range = in.readInt();
        this.gender = in.readInt();
        this.province = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.user_id);
        out.writeInt(this.item_id);
        out.writeInt(this.cat_id);
        out.writeInt(this.merchant_id);
        out.writeInt(this.brand_id);
        out.writeInt(this.month);
        out.writeInt(this.day);
        out.writeInt(this.action);
        out.writeInt(this.age_range);
        out.writeInt(this.gender);
        out.writeUTF(this.province);
    }

    @Override
    public int compareTo(User o) {
        int thisValue = this.action;
        int thatValue = o.action;
        return (thisValue > thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }


}
