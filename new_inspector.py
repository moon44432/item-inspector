import glob
import os
import re
from tqdm import tqdm
import nbtlib
from nbtlib.tag import *
from nbtlib import *
import csv
import parmap
import numpy as np
import mcworldlib as mc


# Specify regions(not chunks) to inspect

region_list = {
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
        if item['id'].unpack() not in items.keys():
            items[item['id'].unpack()] = 0
        try:
            items[item['id'].unpack()] += item['Count']
        except KeyError:
            items[item['id'].unpack()] += item['count']


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
        blockEntity['id'] == String('Furnace') or
        blockEntity['id'] == String('minecraft:furnace') or
        re.search(r'minecraft:[a-z_]*shulker_box', blockEntity['id'])):
        try:
            searchInv(blockEntity['Items'], items)
        except KeyError:
            pass


def searchChunk(data, items):
    try:
        blockEntities = data['block_entities']
    except KeyError:
        try:
            blockEntities = data['Level']['TileEntities']
        except KeyError:
            return

    for blockEntity in blockEntities:
        searchBlockEntity(blockEntity, items)

def worker(worldlist):
    output_c = []
    output_p = []

    for worldpath in tqdm(worldlist):
        regionlist = []
        world_items = {}
        player_items = {}

        for region_xz, date in region_list:
            if int(worldpath[-17:-11]) >= date:
                regionlist.append(region_xz)

        print('Processing ' + worldpath)
        print('Regions: ' + str(regionlist))

        world = mc.load(worldpath)
        regions = world.regions[mc.OVERWORLD]

        for region_xz in tqdm(regionlist):
            try:
                region = regions[region_xz[0], region_xz[1]]
            except:
                print(f"Region {region_xz} not found in {worldpath}")
                continue
            for i in range(32):
                for j in range(32):
                    try:
                        chunk = region[i, j]
                        searchChunk(parse_nbt(chunk.pretty()), world_items)
                    except:
                        print(f"Chunk {i}, {j} not found in region {region_xz} of {worldpath}")
                        continue

        player_nbt_list = glob.glob(worldpath + '/playerdata/*.dat')
        for nbt in tqdm(player_nbt_list):
            data = nbtlib.load(nbt)
            searchPlayer(data, player_items)

        output_c.append({'date': worldpath[-17:-15]+'-'+worldpath[-15:-13]+'-'+worldpath[-13:-11]} | world_items.copy())
        output_p.append({'date': worldpath[-17:-15]+'-'+worldpath[-15:-13]+'-'+worldpath[-13:-11]} | player_items.copy())

    return [output_c, output_p]
    

if __name__ == "__main__":
    # pool = multiprocessing.Pool()
    num_cores = 6

    worldlist = get_world_list('MoonServer')
    print(worldlist)

    # worldlist = worldlist[:4]

    splitted_world_list = np.array_split(worldlist, num_cores)
    splitted_world_list = [x.tolist() for x in splitted_world_list]

    print(splitted_world_list)

    outputs = parmap.map(worker, splitted_world_list, pm_pbar=True, pm_processes=num_cores)

    if os.path.isfile('output_c.csv'):
        fc = open('output_c.csv','w', newline='', buffering=1)
    else:
        fc = open('output_c.csv','x', newline='', buffering=1)

    if os.path.isfile('output_p.csv'):
        fp = open('output_p.csv','w', newline='', buffering=1)
    else:
        fp = open('output_p.csv','x', newline='', buffering=1)

    _outputs_c = []
    _outputs_p = []

    for output_c, output_p in outputs:
        _outputs_c += output_c
        _outputs_p += output_p

    _w = {}
    _p = {}

    for output_c in _outputs_c:
        _w.update(output_c)
    for output_p in _outputs_p:
        _p.update(output_p)

    itemlist = list(_w.keys()) + list(_p.keys())
    for i, item in enumerate(itemlist):
        itemlist[i] = item.replace('minecraft:', '')
    itemlist = sorted(set(itemlist))
    default = dict.fromkeys(['date'] + itemlist, 0)

    wc = csv.DictWriter(fc, fieldnames=default.keys())
    wc.writeheader()

    wp = csv.DictWriter(fp, fieldnames=default.keys())
    wp.writeheader()

    outputs_c = []
    outputs_p = []

    for output_c in tqdm(_outputs_c):
        outputs_c.append(default.copy() | output_c)
    for output_p in tqdm(_outputs_p):
        outputs_p.append(default.copy() | output_p)

    wc.writerows(outputs_c)
    wp.writerows(outputs_p)

    fc.close()
    fp.close()
