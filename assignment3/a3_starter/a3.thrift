service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void replicate(1: string key, 2: string value, 3: i32 primaryOps);
}