// Common seed: each company gets the 1x1 cell at its own point; points are distinct, so no overlaps.
#include <cstdio>
int main() {
    int n;
    if (scanf("%d", &n) != 1) return 0;
    for (int i = 0; i < n; ++i) {
        int x, y, r;
        scanf("%d %d %d", &x, &y, &r);
        printf("%d %d %d %d\n", x, y, x + 1, y + 1);
    }
}
