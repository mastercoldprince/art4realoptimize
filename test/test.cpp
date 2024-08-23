#include <iostream>
#include <array>

using Value = std::array<uint8_t, 1024ULL>;

class Leaf {
public:
    Value value;
    Leaf(const Value& val): value(val) {}
    void set_value(const Value& val) { value = val; }
};

int main() {
    Leaf la = Leaf(Value({'1','2','3'}));
    Value b = {'8','8','8'};
    
    for(int i = 0; i < la.value.size(); i++) {
        std::cout << la.value[i];
    }
    std::cout << std::endl;

    la.set_value(b);

    for(int i = 0; i < la.value.size(); i++) {
        std::cout << la.value[i];
    }
    std::cout << std::endl;

    std::cout << la.value.size() << std::endl;

    return 0;
}