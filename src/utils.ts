
export class Utils {

  static encodeTotalLength(data: number[]) {
    return data[3] | data[2] << 8 | data[1] << 16 | data[0] << 24;
  };

}

