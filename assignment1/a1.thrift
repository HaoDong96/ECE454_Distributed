exception IllegalArgument {
  1: string message;
}

service BcryptService {
 list<string> hashPassword (1: list<string> password, 2: i16 logRounds) throws (1: IllegalArgument e);
 list<bool> checkPassword (1: list<string> password, 2: list<string> hash) throws (1: IllegalArgument e);
 list<string> BEhashPassword (1: list<string> password, 2: i16 logRounds) throws (1: IllegalArgument e);
 list<bool> BEcheckPassword (1: list<string> password, 2: list<string> hash) throws (1: IllegalArgument e);
 void addBE(1: string BEhost, 2: i16 BEport) throws (1: IllegalArgument e);
}
