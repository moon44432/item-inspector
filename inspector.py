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
regionlist = [(-2, -1), (-1, -1), (-2, 0), (-1, 0), (0, 0), (-1, 1), (0, 1), (1, 1), (2, 1), (3, -2)]
    
# Specify items to find. They must be in valid minecraft item ID like minecraft:diamond_block.
itemlist = ['minecraft:diamond',
            'minecraft:diamond_block',
            'minecraft:diamond_ore',
            'minecraft:deepslate_diamond_ore',
            'minecraft:netherite_ingot',
            'minecraft:netherite_scrap',
            'minecraft:netherite_block',
            'minecraft:ancient_debris',
            'minecraft:iron_nugget',
            'minecraft:iron_ingot',
            'minecraft:iron_block',
            'minecraft:raw_iron',
            'minecraft:raw_iron_block',
            'minecraft:iron_ore',
            'minecraft:deepslate_iron_ore',
            'minecraft:gold_nugget',
            'minecraft:gold_ingot',
            'minecraft:gold_block',
            'minecraft:raw_gold',
            'minecraft:raw_gold_block',
            'minecraft:gold_ore',
            'minecraft:deepslate_gold_ore',
            'minecraft:redstone',
            'minecraft:redstone_block',
            'minecraft:redstone_ore',
            'minecraft:deepslate_redstone_ore',
            'minecraft:emerald',
            'minecraft:emerald_block',
            'minecraft:emerald_ore',
            'minecraft:deepslate_emerald_ore',
            'minecraft:coal',
            'minecraft:coal_block'
            'minecraft:coal_ore',
            'minecraft:deepslate_coal_ore',
            'minecraft:quartz',
            'minecraft:quartz_ore',
            'minecraft:nether_quartz_ore',
            ]


def get_world_list(world_name):
    return sorted(glob.glob("./" + world_name + "-*/" + world_name), reverse=True)


def searchInv(inv, items):
    for item in inv:
        if re.search(r'minecraft:[a-z_]*shulker_box', item['id']):
                try:
                    searchInv(item['tag']['BlockEntityTag']['Items'], items)
                except KeyError:
                    pass
        for target in items:
            if (item['id'] == String(target)):
                items[target] += item['Count']


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
        blockEntities = data['']['Level']['TileEntities']

    for blockEntity in blockEntities:
        searchBlockEntity(blockEntity, items)

def worker(worldlist):
    output = []
    world_items = {}
    player_items = {}

    for item in itemlist:
        world_items[item] = 0
        player_items[item] = 0

    for worldpath in worldlist:
        for (rx, ry) in tqdm(regionlist):
            subprocess.call(['./region-parser.sh', worldpath + '/region/r.{0}.{1}.mca'.format(rx, ry)], stdout=subprocess.DEVNULL,
                                stderr=subprocess.STDOUT)
            # stream = os.popen( + worldpath + '/region/r.{0}.{1}.mca'.format(rx, ry))
            # output = stream.read()

        nbt_list = glob.glob(worldpath + '/region/r.*.*.mca_nbt/*.nbt')
        for nbt in tqdm(nbt_list):
            data = nbtlib.load(nbt)
            searchChunk(data, world_items)

        player_nbt_list = glob.glob(worldpath + '/playerdata/*.dat')
        for nbt in tqdm(player_nbt_list):
            data = nbtlib.load(nbt)
            searchPlayer(data, player_items)

        output += ([worldpath[-17:-15]+'-'+worldpath[-15:-13]+'-'+worldpath[-13:-11]] + list(world_items.values()) + list(player_items.values()))
    return output
    

if __name__ == "__main__":
    f = open('write.csv','w', newline='')
    w = csv.writer(f)
    
    w.writerow(['date'] + itemlist*2)

    # pool = multiprocessing.Pool()
    num_cores = 8

    worldlist = get_world_list('MoonServer')
    print(worldlist)

    splitted_world_list = np.array_split(worldlist, num_cores)
    splitted_world_list = [x.tolist() for x in splitted_world_list]

    outputs = parmap.map(worker, splitted_world_list, pm_pbar=True, pm_processes=num_cores)

    for output in outputs:
        w.writerow(output)

    f.close()
