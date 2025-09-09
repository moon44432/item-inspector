import json
import glob
import os
import re
import subprocess
from tqdm import tqdm
import nbtlib
from nbtlib.tag import *
from nbtlib import *
import multiprocessing
import csv
import parmap
import numpy as np
import subprocess


# Specify regions(not chunks) to inspect

regions = {
    ((0, 0), 150503),
    ((0, 1), 150503),
    ((-1, -1), 150503),
    ((-1, 0), 150503),
    ((1, 1), 150503),
    ((-2, 0), 160202),
    ((3, -2), 160202),
    ((0, -1), 171105),
    ((-2, -1), 200701),
    ((-2, 1), 220101),
    ((-1, 1), 220101),
    ((1, 0), 230701),
    ((2, 1), 230701),
    ((-3, 1), 241001),
    ((-7, 2), 241001),
    ((-7, 3), 241001),
    ((-4, -2), 250601),

}
# Specify items to find. They must be in valid minecraft item ID like minecraft:diamond_block.
itemlist = [
    # 광물
    'diamond',
    'diamond_block',
    'diamond_ore',
    'deepslate_diamond_ore',
    'Diamond',

    'netherite_ingot',
    'netherite_scrap',
    'netherite_block',
    'ancient_debris',
    'Netherite',
            
    'iron_nugget',
    'iron_ingot',
    'iron_block',
    'raw_iron',
    'raw_iron_block',
    'iron_ore',
    'deepslate_iron_ore',
    'Iron',

    'gold_nugget',
    'gold_ingot',
    'gold_block',
    'raw_gold',
    'raw_gold_block',
    'gold_ore',
    'deepslate_gold_ore',
    'Gold',

    'redstone',
    'redstone_block',
    'redstone_ore',
    'deepslate_redstone_ore',
    'Redstone',

    'emerald',
    'emerald_block',
    'emerald_ore',
    'deepslate_emerald_ore',
    'Emerald',

    'coal',
    'coal_block',
    'coal_ore',
    'deepslate_coal_ore',
    'charcoal',
    'Coal',

    'copper_ingot',
    'copper_block',
    'raw_copper',
    'raw_copper_block',
    'copper_ore',
    'deepslate_copper_ore',
    'Copper',

    'quartz',
    'quartz_ore',
    'nether_quartz_ore',
    'Quartz',

    # 건축

    'stone',
    'cobblestone',
    'granite',
    'polished_granite',
    'diorite',
    'polished_diorite',
    'andesite',
    'polished_andesite',
    'smooth_stone',
    'Stone',

    'stonebrick',
    'stone_bricks',
    'mossy_stone_bricks',
    'cracked_stone_bricks',
    'chiseled_stone_bricks',
    'Stone Bricks',

    'deepslate',
    'cobbled_deepslate',
    'Deepslate',

    'dirt',
    'coarse_dirt',
    'podzol',
    'Dirt',

    'sand',
    'Sand',

    'sandstone',
    'chiseled_sandstone',
    'cut_sandstone',
    'smooth_sandstone',
    'Sandstone',

    'gravel',
    'Gravel',

    'bricks',
    'brick_block',
    'Bricks',

    'obsidian',
    'Obsidian',

    'snow',
    'snow_block',
    'snowball',
    'Snow',

    'quartz_block',
    'chiseled_quartz_block',
    'quartz_pillar',
    'smooth_quartz',
    'Quartz Block',

    'planks',
    'oak_planks',
    'spruce_planks',
    'birch_planks',
    'jungle_planks',
    'acacia_planks',
    'dark_oak_planks',
    'mangrove_planks',
    'cherry_planks',
    'crimson_planks',
    'warped_planks',
    'Planks',

    'log',
    'log2',
    'oak_log',
    'spruce_log',
    'birch_log',
    'jungle_log',
    'acacia_log',
    'dark_oak_log',
    'mangrove_log',
    'cherry_log',
    'crimson_stem',
    'warped_stem',
    'Wood',

    'glass',
    'stained_glass',
    'white_stained_glass',
    'orange_stained_glass',
    'magenta_stained_glass',
    'light_blue_stained_glass',
    'yellow_stained_glass',
    'lime_stained_glass',
    'pink_stained_glass',
    'gray_stained_glass',
    'light_gray_stained_glass',
    'cyan_stained_glass',
    'purple_stained_glass',
    'blue_stained_glass',
    'brown_stained_glass',
    'green_stained_glass',
    'red_stained_glass',
    'black_stained_glass',
    'Glass',

    'glass_pane',
    'stained_glass_pane',
    'white_stained_glass_pane',
    'orange_stained_glass_pane',
    'magenta_stained_glass_pane',
    'light_blue_stained_glass_pane',
    'yellow_stained_glass_pane',
    'lime_stained_glass_pane',
    'pink_stained_glass_pane',
    'gray_stained_glass_pane',
    'light_gray_stained_glass_pane',
    'cyan_stained_glass_pane',
    'purple_stained_glass_pane',
    'blue_stained_glass_pane',
    'brown_stained_glass_pane',
    'green_stained_glass_pane',
    'red_stained_glass_pane',
    'black_stained_glass_pane',
    'Glass Pane',

    'wool',
    'white_wool',
    'orange_wool',
    'magenta_wool',
    'light_blue_wool',
    'yellow_wool',
    'lime_wool',
    'pink_wool',
    'gray_wool',
    'light_gray_wool',
    'cyan_wool',
    'purple_wool',
    'blue_wool',
    'brown_wool',
    'green_wool',
    'red_wool',
    'black_wool',
    'Wool',

    'stained_hardened_clay',
    'white_terracotta',
    'orange_terracotta',
    'magenta_terracotta',
    'light_blue_terracotta',
    'yellow_terracotta',
    'lime_terracotta',
    'pink_terracotta',
    'gray_terracotta',
    'light_gray_terracotta',
    'cyan_terracotta',
    'purple_terracotta',
    'blue_terracotta',
    'brown_terracotta',
    'green_terracotta',
    'red_terracotta',
    'black_terracotta',
    'hardened_clay',
    'terracotta',
    'Terracotta',

    'concrete',
    'white_concrete',
    'orange_concrete',
    'magenta_concrete',
    'light_blue_concrete',
    'yellow_concrete',
    'lime_concrete',
    'pink_concrete',
    'gray_concrete',
    'light_gray_concrete',
    'cyan_concrete',
    'purple_concrete',
    'blue_concrete',
    'brown_concrete',
    'green_concrete',
    'red_concrete',
    'black_concrete',
    'Concrete',

    'concrete_powder',
    'white_concrete_powder',
    'orange_concrete_powder',
    'magenta_concrete_powder',
    'light_blue_concrete_powder',
    'yellow_concrete_powder',
    'lime_concrete_powder',
    'pink_concrete_powder',
    'gray_concrete_powder',
    'light_gray_concrete_powder',
    'cyan_concrete_powder',
    'purple_concrete_powder',
    'blue_concrete_powder',
    'brown_concrete_powder',
    'green_concrete_powder',
    'red_concrete_powder',
    'black_concrete_powder',
    'Concrete Powder',

    # 농축산물

    'hay_bale',
    'hay_block',
    'wheat',
    'Wheat',

    'wheat_seeds',
    'Seeds',

    'carrot',
    'golden_carrot',
    'Carrot',

    'potato',
    'baked_potato',
    'Potato',

    'beetroot',
    'Beetroot',

    'pumpkin',
    'carved_pumpkin',
    'Pumpkin',

    'reeds',
    'sugar_cane',
    'Sugar Cane',

    'bamboo',
    'Bamboo',

    'porkchop',
    'cooked_porkchop',
    'Porkchop',

    'beef',
    'cooked_beef',
    'Beef',

    'chicken',
    'cooked_chicken',
    'Chicken',

    'mutton',
    'cooked_mutton',
    'Mutton',

    'rabbit',
    'cooked_rabbit',
    'Rabbit',

    'leather',
    'Leather',

    'egg',
    'Egg',

    'nether_wart',
    'Nether Wart',

    # 기타

    'gunpowder',
    'Gunpowder',

    'tnt',
    'TNT',

    'potion',
    'splash_potion',
    'lingering_potion',
    'Potion',

    'skull'
    'skeleton_skull',
    'wither_skeleton_skull',
    'player_head',
    'zombie_head',
    'creeper_head',
    'dragon_head',
    'Skull',

    'enchanted_book',
    'Enchanted Book',

    'book',
    'written_book',
    'writable_book',
    'Book',

]


def get_world_list(world_name):
    return sorted(glob.glob("./worlds/" + world_name + "-*/" + world_name), reverse=True)


def searchInv(inv, items, ver_1_21_and_above=False):
    for item in inv:
        if ver_1_21_and_above:
            item = item['item']
        if re.search(r'minecraft:[a-z_]*shulker_box', item['id']):
                try:
                    searchInv(item['tag']['BlockEntityTag']['Items'], items)
                except KeyError:
                    try:
                        searchInv(item['components']['minecraft:container'], items, ver_1_21_and_above=True)
                    except KeyError:
                        pass
        for target in items:
            if (item['id'] == String('minecraft:' + target)):
                try:
                    items[target] += item['Count']
                except KeyError:
                    items[target] += item['count']


def searchPlayer(data, items):
    searchInv(data['']['Inventory'], items)
    searchInv(data['']['EnderItems'], items)


def searchBlockEntity(blockEntity, items):
    if (blockEntity['id'] == String('minecraft:chest') or
        blockEntity['id'] == String('Chest') or
        blockEntity['id'] == String('minecraft:trapped_chest') or
        blockEntity['id'] == String('Trap') or
        blockEntity['id'] == String('minecraft:dispenser') or
        blockEntity['id'] == String('Dispenser') or
        blockEntity['id'] == String('minecraft:dropper') or
        blockEntity['id'] == String('Dropper') or
        blockEntity['id'] == String('minecraft:hopper') or
        blockEntity['id'] == String('Hopper') or
        blockEntity['id'] == String('minecraft:barrel') or
        re.search(r'minecraft:[a-z_]*shulker_box', blockEntity['id'])):
        try:
            searchInv(blockEntity['Items'], items)
        except KeyError:
            pass


def searchChunk(data, items):
    try:
        blockEntities = data['']['block_entities']
    except KeyError:
        try:
            blockEntities = data['']['Level']['TileEntities']
        except KeyError:
            return

    for blockEntity in blockEntities:
        searchBlockEntity(blockEntity, items)

def worker(worldlist):
    output_c = []
    output_p = []
    world_items = {}
    player_items = {}

    for worldpath in worldlist:
        for item in itemlist:
            world_items[item] = 0
            player_items[item] = 0

        regionlist = []

        for region, date in regions:
            if int(worldpath[-17:-11]) >= date:
                regionlist.append(region)

        # make region list
        for (rx, ry) in tqdm(regionlist):
            subprocess.call(['./region-parser.sh', worldpath + '/region/r.{0}.{1}.mca'.format(rx, ry)], stdout=subprocess.DEVNULL,
                                stderr=subprocess.STDOUT)
            # stream = os.popen( + worldpath + '/region/r.{0}.{1}.mca'.format(rx, ry))
            # output = stream.read()

        print('Processing ' + worldpath)
        print('Regions: ' + str(regionlist))
        nbt_list = glob.glob(worldpath + '/region/r.*.*.mca_nbt/*.nbt')
        for nbt in tqdm(nbt_list):
            data = nbtlib.load(nbt)
            searchChunk(data, world_items)

        player_nbt_list = glob.glob(worldpath + '/playerdata/*.dat')
        for nbt in tqdm(player_nbt_list):
            data = nbtlib.load(nbt)
            searchPlayer(data, player_items)

        output_c.append([worldpath[-17:-15]+'-'+worldpath[-15:-13]+'-'+worldpath[-13:-11]] + list(world_items.values()))
        output_p.append([worldpath[-17:-15]+'-'+worldpath[-15:-13]+'-'+worldpath[-13:-11]] + list(player_items.values()))
    return [output_c, output_p]
    

if __name__ == "__main__":
    if os.path.isfile('output_c.csv'):
        fc = open('output_c.csv','w', newline='', buffering=1)
    else:
        fc = open('output_c.csv','x', newline='', buffering=1)

    if os.path.isfile('output_p.csv'):
        fp = open('output_p.csv','w', newline='', buffering=1)
    else:
        fp = open('output_p.csv','x', newline='', buffering=1)
    
    wc = csv.writer(fc)
    wc.writerow(['date'] + itemlist)

    wp = csv.writer(fp)
    wp.writerow(['date'] + itemlist)

    # pool = multiprocessing.Pool()
    num_cores = 6

    worldlist = get_world_list('MoonServer')
    print(worldlist)

    # worldlist = worldlist[:4]

    splitted_world_list = np.array_split(worldlist, num_cores)
    splitted_world_list = [x.tolist() for x in splitted_world_list]

    print(splitted_world_list)

    outputs = parmap.map(worker, splitted_world_list, pm_pbar=True, pm_processes=num_cores)

    for output_c, output_p in outputs:
        wc.writerows(output_c)
        wp.writerows(output_p)

        fc.flush()
        fp.flush()

    fc.close()
    fp.close()
