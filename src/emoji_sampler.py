import random
from typing import Any

class FenwickTree:
    def __init__(self, data):
        self.n = len(data)
        self.tree = [0] * (self.n + 1)

        # linear construction
        for i in range(1, self.n + 1):
            # current cell
            self.tree[i] += data[i - 1]

            # get neighbor
            r = i + (i & -i)

            # if neighbor is valid
            if(r <= self.n):
                self.tree[r] += self.tree[i]

    def update(self, i, delta):
        i += 1
        while i <= self.n:
            self.tree[i] += delta
            # propagate
            i += i & -i

    def query(self, i):
        i += 1
        res = 0
        while i > 0:
            res += self.tree[i]
            i -= i & -i
        return res
    
    def find_prefix_index(self, target):
        idx = 0
        bit_mask = 1 << (self.n.bit_length()) # Largest power of 2 <= n

        while bit_mask:
            next_idx = idx + bit_mask
            if next_idx <= self.n and self.tree[next_idx] < target:
                target -= self.tree[next_idx]
                idx = next_idx
            bit_mask >>= 1
        return idx
    
class Sampler:
    """sample without replacement from a map of frequencies"""
    def __init__(self, frequencies: dict[Any, int], seed = None) -> Any:
        self._emoji_list = list(frequencies.keys())
        self._values = list(frequencies.values())
        self._total_emojis = sum(self._values)
        self.tree = FenwickTree(self._values)
        self._rng = random.Random(seed)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.has_more():
            raise StopIteration
        return self.sample()

    def has_more(self):
        return self._total_emojis > 0

    def sample(self):
        for _ in range(self._total_emojis):
            choice = self._rng.randint(1, self._total_emojis)
            idx = self.tree.find_prefix_index(choice)
            self.decrease_emoji(idx)
            return self._emoji_list[idx]

    def decrease_emoji(self, idx):
        self.tree.update(idx, -1)
        self._total_emojis -= 1

# emojis_dict = {
#     "smile": 6,
#     "pleading_face": 4,
#     "sad_face": 2
# }

# s = Sampler(emojis_dict)
# for emoji in s:
#     print(emoji)